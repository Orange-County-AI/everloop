package main

// Delivery sinks: how a drained spool message reaches the agent session.
// `serve` is harness-agnostic — the MCP tools and the spool protocol are the
// same everywhere; only the last hop differs. CHANNEL_SINK selects it:
//
//	claude   (default) MCP notifications/claude/channel — Claude Code channels
//	none               tools-only serve: no draining, no presence; pair with a
//	                   standalone `tincan pump` that owns delivery ("tools" works too)
//	opencode           POST {OPENCODE_URL}/session/{id}/prompt_async — pushes a
//	                   real user turn into a persistent opencode session
//	hermes             POST {HERMES_WEBHOOK_URL} — a Hermes webhook route (V2
//	                   HMAC); each event spawns a run (Hermes has no persistent
//	                   session to inject into)
//	herdr              exec `herdr agent prompt {HERDR_TARGET} <envelope>
//	                   --wait --timeout N` — types the event into whatever agent
//	                   herdr is driving in that pane, so the transport is
//	                   model-agnostic (no harness-specific plane to depend on)
//
// Non-claude sinks wrap the message in the same `<channel ...meta>content
// </channel>` envelope Claude Code produces, so the server's instructions text
// describes the format accurately on every harness. Delivery failures leave
// the message claimed-but-unacked, so the spool's at-least-once reclaim
// retries it on the next poll.

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type sink interface {
	deliver(content string, meta map[string]string) error
}

// newSink picks the delivery path from env. `source` is the channel name used
// in the envelope ("tincan" / "everloop"); `out` is the MCP stdout writer the
// claude sink notifies on.
func newSink(source string, out *stdoutWriter) (sink, error) {
	switch strings.ToLower(os.Getenv("CHANNEL_SINK")) {
	case "", "claude":
		return &claudeSink{out: out}, nil
	case "none", "tools":
		// Tools-only: serve exposes send_message/list_peers but never drains
		// (and never heartbeats presence) — a standalone `tincan pump` owns
		// delivery for the mailbox. Without this, mounting serve next to a
		// pump would ack messages into a harness that ignores channel
		// notifications, silently losing them.
		return nil, nil
	case "opencode":
		return &opencodeSink{
			base:      strings.TrimRight(envOr("OPENCODE_URL", "http://127.0.0.1:4096"), "/"),
			sessionID: os.Getenv("OPENCODE_SESSION_ID"),
			title:     envOr("OPENCODE_SESSION_TITLE", "channel"),
			directory: os.Getenv("OPENCODE_DIRECTORY"),
			username:  envOr("OPENCODE_SERVER_USERNAME", "opencode"),
			password:  os.Getenv("OPENCODE_SERVER_PASSWORD"),
			source:    source,
			client:    &http.Client{Timeout: 15 * time.Second},
		}, nil
	case "hermes":
		url := os.Getenv("HERMES_WEBHOOK_URL")
		if url == "" {
			return nil, fmt.Errorf("CHANNEL_SINK=hermes requires HERMES_WEBHOOK_URL (e.g. http://127.0.0.1:8644/webhooks/%s)", source)
		}
		return &hermesSink{
			url:    url,
			secret: os.Getenv("HERMES_WEBHOOK_SECRET"),
			source: source,
			client: &http.Client{Timeout: 15 * time.Second},
		}, nil
	case "herdr":
		target := os.Getenv("HERDR_TARGET")
		if target == "" {
			return nil, fmt.Errorf("CHANNEL_SINK=herdr requires HERDR_TARGET (a herdr agent target: pane id, agent name, or workspace/tab path)")
		}
		timeoutMS := herdrDefaultTimeoutMS
		if v := os.Getenv("HERDR_PROMPT_TIMEOUT_MS"); v != "" {
			// Refuse a bad value rather than quietly falling back: a typo here
			// would otherwise wait two minutes per event and look like a slow
			// agent, with nothing saying the setting never took.
			n, err := strconv.Atoi(strings.TrimSpace(v))
			if err != nil || n < 1 {
				return nil, fmt.Errorf("invalid HERDR_PROMPT_TIMEOUT_MS %q: want a positive integer (milliseconds)", v)
			}
			timeoutMS = n
		}
		return &herdrSink{
			bin:       envOr("HERDR_BIN", "herdr"),
			target:    target,
			timeoutMS: timeoutMS,
			source:    source,
		}, nil
	default:
		return nil, fmt.Errorf("unknown CHANNEL_SINK %q: want claude|opencode|hermes|herdr", os.Getenv("CHANNEL_SINK"))
	}
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// envelope renders the exact `<channel ...>` wrapper Claude Code puts around a
// channel notification: every non-empty meta entry becomes an attribute.
func envelope(source, content string, meta map[string]string) string {
	var b strings.Builder
	b.WriteString("<channel source=\"")
	b.WriteString(xmlAttrEscape(source))
	b.WriteByte('"')
	keys := make([]string, 0, len(meta))
	for k, v := range meta {
		if v != "" && k != "source" {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	for _, k := range keys {
		fmt.Fprintf(&b, " %s=\"%s\"", k, xmlAttrEscape(meta[k]))
	}
	b.WriteString(">\n")
	b.WriteString(content)
	b.WriteString("\n</channel>")
	return b.String()
}

var xmlAttrEscaper = strings.NewReplacer(
	"&", "&amp;",
	"<", "&lt;",
	">", "&gt;",
	`"`, "&quot;",
)

func xmlAttrEscape(s string) string { return xmlAttrEscaper.Replace(s) }

// --- claude (default): MCP channel notification, unchanged behavior ----------

type claudeSink struct{ out *stdoutWriter }

func (s *claudeSink) deliver(content string, meta map[string]string) error {
	s.out.notify("notifications/claude/channel", map[string]any{
		"content": content,
		"meta":    meta,
	})
	return nil
}

// --- opencode: user-turn injection over HTTP ---------------------------------

type opencodeSink struct {
	base      string
	sessionID string // explicit override; skip resolution when set
	title     string
	directory string
	username  string // opencode's own basic-auth envs; password empty = no auth
	password  string
	source    string
	client    *http.Client

	mu       sync.Mutex
	resolved string
}

// do issues an authenticated request; opencode enables HTTP basic auth when
// OPENCODE_SERVER_PASSWORD is set on the server.
func (s *opencodeSink) do(method, path string, body io.Reader) (*http.Response, error) {
	req, err := http.NewRequest(method, s.base+path, body)
	if err != nil {
		return nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if s.password != "" {
		req.SetBasicAuth(s.username, s.password)
	}
	return s.client.Do(req)
}

type ocSession struct {
	ID        string `json:"id"`
	Title     string `json:"title"`
	Directory string `json:"directory"`
}

func (s *opencodeSink) session() (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.sessionID != "" {
		return s.sessionID, nil
	}
	if s.resolved != "" {
		return s.resolved, nil
	}
	res, err := s.do(http.MethodGet, "/session", nil)
	if err != nil {
		return "", fmt.Errorf("opencode GET /session: %w", err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return "", fmt.Errorf("opencode GET /session: HTTP %d", res.StatusCode)
	}
	var sessions []ocSession
	if err := json.NewDecoder(res.Body).Decode(&sessions); err != nil {
		return "", err
	}
	for _, sess := range sessions {
		if sess.Title == s.title && (s.directory == "" || sess.Directory == s.directory) {
			s.resolved = sess.ID
			return sess.ID, nil
		}
	}
	body, _ := json.Marshal(map[string]any{"title": s.title})
	res2, err := s.do(http.MethodPost, "/session", bytes.NewReader(body))
	if err != nil {
		return "", fmt.Errorf("opencode POST /session: %w", err)
	}
	defer res2.Body.Close()
	if res2.StatusCode/100 != 2 {
		return "", fmt.Errorf("opencode POST /session: HTTP %d", res2.StatusCode)
	}
	var created ocSession
	if err := json.NewDecoder(res2.Body).Decode(&created); err != nil {
		return "", err
	}
	s.resolved = created.ID
	return created.ID, nil
}

func (s *opencodeSink) deliver(content string, meta map[string]string) error {
	text := envelope(s.source, content, meta)
	body, _ := json.Marshal(map[string]any{
		"parts": []map[string]any{{"type": "text", "text": text}},
	})
	post := func(sessionID string) (*http.Response, error) {
		return s.do(http.MethodPost, "/session/"+sessionID+"/prompt_async", bytes.NewReader(body))
	}
	sid, err := s.session()
	if err != nil {
		return err
	}
	res, err := post(sid)
	if err != nil {
		return fmt.Errorf("opencode prompt_async: %w", err)
	}
	if res.StatusCode == http.StatusNotFound && s.sessionID == "" {
		// Cached session vanished (deleted/restarted server) — re-resolve once.
		res.Body.Close()
		s.mu.Lock()
		s.resolved = ""
		s.mu.Unlock()
		if sid, err = s.session(); err != nil {
			return err
		}
		if res, err = post(sid); err != nil {
			return fmt.Errorf("opencode prompt_async: %w", err)
		}
	}
	defer res.Body.Close()
	if res.StatusCode/100 != 2 {
		snippet, _ := io.ReadAll(io.LimitReader(res.Body, 300))
		return fmt.Errorf("opencode prompt_async: HTTP %d: %s", res.StatusCode, strings.TrimSpace(string(snippet)))
	}
	return nil
}

// --- hermes: webhook route (spawns a run per event) --------------------------

type hermesSink struct {
	url    string
	secret string
	source string
	client *http.Client
}

func (s *hermesSink) deliver(content string, meta map[string]string) error {
	payload, _ := json.Marshal(map[string]any{
		"body": envelope(s.source, content, meta),
		"meta": meta,
	})
	req, err := http.NewRequest(http.MethodPost, s.url, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	// Hermes dedups deliveries on this header for 1h — pairs with the spool's
	// at-least-once redelivery to make sink crashes idempotent.
	if id := meta["event_id"]; id != "" {
		req.Header.Set("X-Request-ID", s.source+"-"+id)
	}
	if s.secret != "" {
		// Generic V2: HMAC-SHA256 hex of "<timestamp>.<body>".
		ts := strconv.FormatInt(time.Now().Unix(), 10)
		mac := hmac.New(sha256.New, []byte(s.secret))
		mac.Write([]byte(ts + "." + string(payload)))
		req.Header.Set("X-Webhook-Timestamp", ts)
		req.Header.Set("X-Webhook-Signature-V2", hex.EncodeToString(mac.Sum(nil)))
	}
	res, err := s.client.Do(req)
	if err != nil {
		return fmt.Errorf("hermes webhook: %w", err)
	}
	defer res.Body.Close()
	if res.StatusCode/100 != 2 {
		snippet, _ := io.ReadAll(io.LimitReader(res.Body, 300))
		return fmt.Errorf("hermes webhook: HTTP %d: %s", res.StatusCode, strings.TrimSpace(string(snippet)))
	}
	return nil
}

// --- herdr: exec the CLI, which types the event into the agent's pane --------

// herdrSink delivers by running `herdr agent prompt <target> <envelope>`. herdr
// drives the agent at the terminal, so the transport says nothing about which
// model is on the other end — the point of this sink, versus the claude one's
// dependency on a harness-specific notification plane.
//
// The command is built as an argv slice and handed to os/exec directly: there
// is no shell anywhere on this path, so an envelope carrying `$(...)`, quotes,
// backticks or newlines is passed through as one literal argument.
type herdrSink struct {
	bin       string
	target    string
	timeoutMS int
	source    string
}

// herdrDefaultTimeoutMS is what `--wait` gets when HERDR_PROMPT_TIMEOUT_MS is
// unset: long enough for an agent to finish a real turn, short enough that a
// wedged one does not hold the drain loop past the next few polls.
const herdrDefaultTimeoutMS = 120000

// herdrHardStopGrace bounds the *process* past herdr's own --wait deadline. If
// the socket is gone or the server is not answering, herdr can block before its
// timeout ever applies, and drainLoop is single-threaded — one wedged delivery
// stalls every message behind it. Same shape as the systemd units'
// TimeoutStartSec = timeout + 30s: the inner deadline is the real one, this is
// only the cover for it not firing.
const herdrHardStopGrace = 30 * time.Second

func (s *herdrSink) deliver(content string, meta map[string]string) error {
	text := envelope(s.source, content, meta)
	// --timeout is refused by herdr without --wait, so the two travel together.
	args := []string{"agent", "prompt", s.target, text, "--wait", "--timeout", strconv.Itoa(s.timeoutMS)}

	ctx, cancel := context.WithTimeout(context.Background(),
		time.Duration(s.timeoutMS)*time.Millisecond+herdrHardStopGrace)
	defer cancel()

	cmd := exec.CommandContext(ctx, s.bin, args...)
	// cmd.Env stays nil, i.e. os.Environ(): HERDR_SOCKET_PATH and HERDR_SESSION
	// reach the CLI untouched and it resolves the server and session with them
	// natively. Reading them here to rebuild that resolution would be a second
	// copy of it, free to drift from the one that actually decides.
	var stderr bytes.Buffer
	cmd.Stdout = io.Discard
	cmd.Stderr = &stderr

	err := cmd.Run()
	if err == nil {
		// Exit 0 means herdr submitted the prompt and observed a settled
		// lifecycle state — which includes *blocked* (the agent stopped on a
		// permission prompt), not only idle/done. So this is evidence the tick
		// was delivered, not proof it was processed. Acking anyway is the same
		// standing the claude sink has always had (a fire-and-forget
		// notification nothing confirms was read), and everloop's design
		// already absorbs it: ticks coalesce, so the next delivery carries the
		// catch-up count rather than the work being lost.
		return nil
	}
	// Anything else leaves the message claimed-but-unacked: the spool reclaims
	// it on the next poll and drainLoop stops the batch here to preserve order.
	detail := strings.TrimSpace(stderr.String())
	if len(detail) > 300 {
		detail = detail[:300]
	}
	// herdr reports failures as JSON on stderr ({"error":{"code":...}}), which
	// names the cause — agent_not_found, agent_prompt_stalled, timeout — where
	// the exit status alone says only "non-zero".
	if ctx.Err() != nil {
		return fmt.Errorf("herdr agent prompt: killed after %s%s", time.Duration(s.timeoutMS)*time.Millisecond+herdrHardStopGrace, herdrDetail(detail))
	}
	return fmt.Errorf("herdr agent prompt: %v%s", err, herdrDetail(detail))
}

func herdrDetail(detail string) string {
	if detail == "" {
		return ""
	}
	return ": " + detail
}
