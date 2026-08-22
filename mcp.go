package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"
)

// Hand-rolled MCP server over stdio (newline-delimited JSON-RPC 2.0).
// We implement the protocol directly rather than via an SDK to keep the binary
// dependency-free; the tools below are ordinary MCP and work on any harness.
//
// Nothing here is Claude-specific any more. Events do not arrive as an MCP
// notification at all — herdr submits them as session input (see sink.go), so
// this server's only job is the loop-management tools plus the spool drain.

const serverInstructions = "Events from the everloop channel are delivered into this session as " +
	`<channel source="everloop" kind="tick|message" ...>, as ordinary input rather than as an MCP notification. ` +
	`kind="tick" is a persistent scheduled loop firing: perform the instruction in the body. ` +
	`If coalesced_count is greater than 1, the loop fired that many times while no session was listening - catch up ONCE, do not repeat the work N times. ` +
	`A command loop's body is its command's output instead: coalesced_count is how many firings produced output, each shown under its own "[everloop] run N of M" header in the order it happened - handle every one, they are different events, not repeats. ` +
	`status="error" or status="timeout" means the loop's command is failing rather than reporting: the body is a diagnostic, not an instruction. Failures are damped (1st, 2nd, 4th, 8th... consecutive), so one report can stand for many silent failures. ` +
	`kind="message" is an ad-hoc message pushed from the "everloop send" CLI by the operator or another process. ` +
	"Delivery is one-way: act on events, no reply expected. " +
	`On the default Transit transport the channel envelope arrives inside a transit/1 envelope, whose "from" is the session everloop runs in - it is still a one-way everloop event, so do NOT reply to it. ` +
	"Manage loops with the create_loop / list_loops / update_loop / delete_loop tools; loops are scheduled outside this session (a systemd user timer, a launchd agent, or the everloop scheduler daemon) and never expire."

type rpcRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type stdoutWriter struct {
	mu  sync.Mutex
	enc *json.Encoder
}

func (w *stdoutWriter) write(v any) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.enc.Encode(v) // Encode appends the required trailing newline
}

func (w *stdoutWriter) result(id json.RawMessage, result any) {
	w.write(map[string]any{"jsonrpc": "2.0", "id": id, "result": result})
}

func (w *stdoutWriter) error(id json.RawMessage, code int, msg string) {
	w.write(map[string]any{"jsonrpc": "2.0", "id": id, "error": rpcError{Code: code, Message: msg}})
}

func pollInterval() time.Duration {
	if s := os.Getenv("EVERLOOP_POLL_SECONDS"); s != "" {
		if n, err := strconv.Atoi(s); err == nil && n >= 1 {
			return time.Duration(n) * time.Second
		}
	}
	return 2 * time.Second
}

func serve() error {
	if err := ensureDirs(); err != nil {
		return err
	}
	warnIfNoScheduler()
	out := &stdoutWriter{enc: json.NewEncoder(os.Stdout)}
	dlv, err := newSink("everloop")
	if err != nil {
		return err
	}
	startPolling := sync.OnceFunc(func() {
		if dlv != nil { // nil = tools-only (CHANNEL_SINK=none): a pump owns delivery
			go drainLoop(dlv)
		}
	})

	scanner := bufio.NewScanner(os.Stdin)
	scanner.Buffer(make([]byte, 0, 64*1024), 8*1024*1024)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		var req rpcRequest
		if err := json.Unmarshal(line, &req); err != nil {
			out.error(nil, -32700, "parse error")
			continue
		}
		switch req.Method {
		case "initialize":
			var p struct {
				ProtocolVersion string `json:"protocolVersion"`
			}
			json.Unmarshal(req.Params, &p)
			if p.ProtocolVersion == "" {
				p.ProtocolVersion = "2024-11-05"
			}
			out.result(req.ID, map[string]any{
				"protocolVersion": p.ProtocolVersion,
				"capabilities": map[string]any{
					// No experimental channel capability: this server no longer
					// pushes notifications of any kind, on any harness.
					"tools": map[string]any{},
				},
				"serverInfo":   map[string]any{"name": "everloop", "version": version},
				"instructions": serverInstructions,
			})
			startPolling() // don't rely on the client sending notifications/initialized
		case "notifications/initialized":
			startPolling()
		case "ping":
			out.result(req.ID, map[string]any{})
		case "tools/list":
			out.result(req.ID, map[string]any{"tools": toolDefs()})
		case "tools/call":
			handleToolCall(out, req)
		default:
			if req.ID != nil {
				out.error(req.ID, -32601, "method not found: "+req.Method)
			}
		}
	}
	return scanner.Err()
}

// drainLoop polls the spool and delivers each claimed message into the
// session via the configured sink (CHANNEL_SINK). Delivery order: claim ->
// deliver -> ack; an unacked (claimed) message is re-claimed on the next
// poll, so a crash or a failing sink redelivers (at-least-once) and the
// message ID doubles as an idempotency key.
func drainLoop(dlv sink) {
	ticker := time.NewTicker(pollInterval())
	defer ticker.Stop()
	for {
		msgs, err := claimPending()
		if err != nil {
			fmt.Fprintf(os.Stderr, "everloop: drain: %v\n", err)
		}
		for _, c := range msgs {
			meta := map[string]string{
				"kind":      c.msg.Kind,
				"event_id":  c.msg.ID,
				"queued_at": c.msg.FirstAt.Format(time.RFC3339),
			}
			if c.msg.Loop != "" {
				meta["loop"] = c.msg.Loop
			}
			if c.msg.Kind == "tick" {
				meta["coalesced_count"] = strconv.Itoa(c.msg.Count)
			}
			if s := c.msg.status(); s != "" {
				meta["status"] = s
			}
			for k, v := range c.msg.Meta {
				meta[k] = v
			}
			if err := dlv.deliver(c.msg.Content, meta); err != nil {
				// Leave claimed: retried next poll. Break to preserve order
				// and avoid hammering a down sink with the rest of the batch.
				fmt.Fprintf(os.Stderr, "everloop: deliver %s failed (retrying next poll): %v\n", c.msg.ID, err)
				break
			}
			c.ack()
		}
		<-ticker.C
	}
}

// --- tools -------------------------------------------------------------------

func toolDefs() []map[string]any {
	str := func(desc string) map[string]any { return map[string]any{"type": "string", "description": desc} }
	obj := func(props map[string]any, required ...string) map[string]any {
		s := map[string]any{"type": "object", "properties": props}
		if len(required) > 0 {
			s["required"] = required
		}
		return s
	}
	return []map[string]any{
		{
			"name": "create_loop",
			"description": "Create a persistent recurring loop, scheduled outside this session. It never expires and survives reboots and session restarts. Provide exactly one of `every` (interval) or `calendar` (OnCalendar expression), and at least one of `message` or `command`.\n\n" +
				"Without `command` the loop is a heartbeat: every firing delivers `message` into this session.\n" +
				"With `command` it is a watch: the command runs on each firing and an event is delivered ONLY if it wrote to stdout — a silent command means no event at all. Prefer this whenever the loop would otherwise start with \"check whether X changed\": let the command do the detecting and stay quiet. `message` then becomes an optional standing instruction shown above the output.",
			"inputSchema": obj(map[string]any{
				"name":     str("Loop name: lowercase letters, digits, hyphens (max 41 chars)"),
				"message":  str("Instruction delivered on each firing; with `command` set, a preamble above the command's output"),
				"command":  str("Shell command run on each firing (sh -c). Exit 0 with empty stdout delivers nothing; exit 0 with output delivers it; non-zero exit delivers a failure report, damped to the 1st/2nd/4th/8th... consecutive failure. Runs in the scheduler's environment, NOT a login shell (~/.profile is not sourced) — use absolute paths."),
				"timeout":  str("Max command runtime like 30s, 2m (default 60s, range 1s..1h). A timeout is reported as a damped failure."),
				"every":    str("Interval like 90s, 5m, 1h30m, 2d (min 10s). Mutually exclusive with calendar."),
				"calendar": str("OnCalendar expression like 'Mon..Fri 09:00', 'daily' or '*-*-* 09:00:00'. A fire missed while the machine was down runs once when it comes back. Mutually exclusive with every."),
				"enabled":  map[string]any{"type": "boolean", "description": "Start the timer immediately (default true)"},
			}, "name"),
		},
		{
			"name":        "list_loops",
			"description": "List all persistent loops with their schedule, enabled state, command (if any), and live timer status. The status names the scheduler backend holding each loop, and says so loudly when nothing is scheduling it.",
			"inputSchema": obj(map[string]any{}),
		},
		{
			"name":        "update_loop",
			"description": "Update a loop's message, command, schedule, or enabled state. Setting `every` clears `calendar` and vice versa. Interval changes reschedule from now. Passing an empty string for `command` clears it, turning a watch back into a plain heartbeat.",
			"inputSchema": obj(map[string]any{
				"name":     str("Name of the loop to update"),
				"message":  str("New message"),
				"command":  str("New command; empty string clears it"),
				"timeout":  str("New command timeout like 30s, 2m"),
				"every":    str("New interval like 90s, 5m, 1h30m, 2d"),
				"calendar": str("New OnCalendar expression"),
				"enabled":  map[string]any{"type": "boolean", "description": "Enable or disable the timer"},
			}, "name"),
		},
		{
			"name":        "delete_loop",
			"description": "Delete a loop: stops and removes its timer and discards any pending tick.",
			"inputSchema": obj(map[string]any{"name": str("Name of the loop to delete")}, "name"),
		},
		{
			"name":        "send_message",
			"description": "Spool an ad-hoc message into the everloop queue. It is delivered back into the session as a channel event on the next poll (useful as a durable deferred note that survives session restarts).",
			"inputSchema": obj(map[string]any{"content": str("The message content")}, "content"),
		},
	}
}

// toolLoopArgs is the create_loop/update_loop argument shape. Embedding
// loopSpec means the tools and the CLI reach validation through exactly the
// same path, and an omitted key stays nil rather than becoming "" — which is
// what lets `command: ""` mean "clear it" without an omitted `command`
// wiping one.
type toolLoopArgs struct {
	Name string
	loopSpec
}

func handleToolCall(out *stdoutWriter, req rpcRequest) {
	var call struct {
		Name      string          `json:"name"`
		Arguments json.RawMessage `json:"arguments"`
	}
	if err := json.Unmarshal(req.Params, &call); err != nil {
		out.error(req.ID, -32602, "invalid params")
		return
	}
	text, err := dispatchTool(call.Name, call.Arguments)
	if err != nil {
		out.result(req.ID, map[string]any{
			"content": []map[string]any{{"type": "text", "text": "Error: " + err.Error()}},
			"isError": true,
		})
		return
	}
	out.result(req.ID, map[string]any{
		"content": []map[string]any{{"type": "text", "text": text}},
	})
}

func dispatchTool(name string, args json.RawMessage) (string, error) {
	switch name {
	case "create_loop":
		var a toolLoopArgs
		if err := json.Unmarshal(args, &a); err != nil {
			return "", err
		}
		l, err := createLoop(a.Name, a.loopSpec)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("Created loop %q (%s). Timer: %s", l.Name, scheduleDesc(l), timerStatus(l.Name)), nil
	case "list_loops":
		loops, err := listLoops()
		if err != nil {
			return "", err
		}
		if len(loops) == 0 {
			return "No loops defined.", nil
		}
		var b []byte
		for _, l := range loops {
			b = fmt.Appendf(b, "- %s: %s | enabled=%v | timer=%s | message=%q",
				l.Name, scheduleDesc(l), l.Enabled, timerStatus(l.Name), l.Message)
			if l.Command != "" {
				b = fmt.Appendf(b, " | command=%q (timeout %s)", l.Command, timeoutDesc(l))
			}
			b = append(b, '\n')
		}
		return string(b), nil
	case "update_loop":
		var a toolLoopArgs
		if err := json.Unmarshal(args, &a); err != nil {
			return "", err
		}
		l, err := updateLoop(a.Name, a.loopSpec)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("Updated loop %q (%s, enabled=%v). Timer: %s", l.Name, scheduleDesc(l), l.Enabled, timerStatus(l.Name)), nil
	case "delete_loop":
		var a struct{ Name string }
		if err := json.Unmarshal(args, &a); err != nil {
			return "", err
		}
		if err := deleteLoop(a.Name); err != nil {
			return "", err
		}
		return fmt.Sprintf("Deleted loop %q.", a.Name), nil
	case "send_message":
		var a struct{ Content string }
		if err := json.Unmarshal(args, &a); err != nil {
			return "", err
		}
		if a.Content == "" {
			return "", fmt.Errorf("content is required")
		}
		if err := enqueueMessage(a.Content, nil); err != nil {
			return "", err
		}
		return "Message spooled; it will be delivered on the next poll.", nil
	default:
		return "", fmt.Errorf("unknown tool: %s", name)
	}
}

func scheduleDesc(l *Loop) string {
	if l.Calendar != "" {
		return "calendar: " + l.Calendar
	}
	return "every " + l.Every
}

// timeoutDesc renders the effective command timeout, including the default the
// loop never had to spell out.
func timeoutDesc(l *Loop) string {
	d, err := parseTimeout(l.Timeout)
	if err != nil {
		return l.Timeout
	}
	return d.String()
}
