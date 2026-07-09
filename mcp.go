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
// We implement the protocol directly rather than via an SDK because the
// channel contract needs a custom capability (claude/channel) and a custom
// notification method, and because it keeps the binary dependency-free.

const serverInstructions = "Events from the everloop channel arrive as " +
	`<channel source="everloop" kind="tick|message" ...>. ` +
	`kind="tick" is a persistent systemd-timer loop firing: perform the instruction in the body. ` +
	`If coalesced_count is greater than 1, the loop fired that many times while no session was listening - catch up ONCE, do not repeat the work N times. ` +
	`kind="message" is an ad-hoc message pushed from the "everloop send" CLI by the operator or another process. ` +
	"The channel is one-way: act on events, no reply expected. " +
	"Manage loops with the create_loop / list_loops / update_loop / delete_loop tools; loops are backed by systemd user timers and never expire."

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

func (w *stdoutWriter) notify(method string, params any) {
	w.write(map[string]any{"jsonrpc": "2.0", "method": method, "params": params})
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
	out := &stdoutWriter{enc: json.NewEncoder(os.Stdout)}
	startPolling := sync.OnceFunc(func() { go drainLoop(out) })

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
					"experimental": map[string]any{"claude/channel": map[string]any{}},
					"tools":        map[string]any{},
				},
				"serverInfo":   map[string]any{"name": "everloop", "version": version},
				"instructions": serverInstructions,
			})
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

// drainLoop polls the spool and pushes each claimed message into the session.
// Delivery order: claim -> notify -> ack, so a crash mid-delivery redelivers
// (at-least-once); the message ID doubles as an idempotency key.
func drainLoop(out *stdoutWriter) {
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
			for k, v := range c.msg.Meta {
				meta[k] = v
			}
			out.notify("notifications/claude/channel", map[string]any{
				"content": c.msg.Content,
				"meta":    meta,
			})
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
			"name":        "create_loop",
			"description": "Create a persistent recurring loop backed by a systemd user timer. It never expires and survives reboots. Provide exactly one of `every` (interval) or `calendar` (systemd OnCalendar expression). Each firing delivers the message into this session as a channel event.",
			"inputSchema": obj(map[string]any{
				"name":     str("Loop name: lowercase letters, digits, hyphens (max 41 chars)"),
				"message":  str("The instruction delivered to the session on each firing"),
				"every":    str("Interval like 90s, 5m, 1h30m, 2d (min 10s). Mutually exclusive with calendar."),
				"calendar": str("systemd OnCalendar expression like 'Mon..Fri 09:00' or 'daily'. Missed fires run at next boot. Mutually exclusive with every."),
				"enabled":  map[string]any{"type": "boolean", "description": "Start the timer immediately (default true)"},
			}, "name", "message"),
		},
		{
			"name":        "list_loops",
			"description": "List all persistent loops with their schedule, enabled state, and live systemd timer status.",
			"inputSchema": obj(map[string]any{}),
		},
		{
			"name":        "update_loop",
			"description": "Update a loop's message, schedule, or enabled state. Setting `every` clears `calendar` and vice versa. Interval changes reschedule from now.",
			"inputSchema": obj(map[string]any{
				"name":     str("Name of the loop to update"),
				"message":  str("New message"),
				"every":    str("New interval like 90s, 5m, 1h30m, 2d"),
				"calendar": str("New systemd OnCalendar expression"),
				"enabled":  map[string]any{"type": "boolean", "description": "Enable or disable the timer"},
			}, "name"),
		},
		{
			"name":        "delete_loop",
			"description": "Delete a loop: stops and removes its systemd timer and discards any pending tick.",
			"inputSchema": obj(map[string]any{"name": str("Name of the loop to delete")}, "name"),
		},
		{
			"name":        "send_message",
			"description": "Spool an ad-hoc message into the everloop queue. It is delivered back into the session as a channel event on the next poll (useful as a durable deferred note that survives session restarts).",
			"inputSchema": obj(map[string]any{"content": str("The message content")}, "content"),
		},
	}
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
		var a struct {
			Name, Message, Every, Calendar string
			Enabled                        *bool
		}
		if err := json.Unmarshal(args, &a); err != nil {
			return "", err
		}
		enabled := a.Enabled == nil || *a.Enabled
		l, err := createLoop(a.Name, a.Message, a.Every, a.Calendar, enabled)
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
			b = fmt.Appendf(b, "- %s: %s | enabled=%v | timer=%s | message=%q\n",
				l.Name, scheduleDesc(l), l.Enabled, timerStatus(l.Name), l.Message)
		}
		return string(b), nil
	case "update_loop":
		var a struct {
			Name     string
			Message  *string
			Every    *string
			Calendar *string
			Enabled  *bool
		}
		if err := json.Unmarshal(args, &a); err != nil {
			return "", err
		}
		l, err := updateLoop(a.Name, a.Message, a.Every, a.Calendar, a.Enabled)
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
