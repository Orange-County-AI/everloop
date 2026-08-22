package main

// Transit's local IPC socket — the second transport a drained message can
// travel on, added during the fleet's migration off herdr.
//
// WHY THE SOCKET AND NOT A CLI. There is no `transit send`. Transit's CLI
// dispatches daemon|adapter|enroll|status|inbox|pause|mcp|version and nothing
// else (transit/daemon/main.go run()). The only sending surface it ships is the
// MCP tool `send_message`, and that tool is itself a thin wrapper over
// `daemonCall({"op":"send",...})` on this same unix socket
// (transit/daemon/mcp.go mcpCallAsPane). "Use the CLI" would therefore mean
// spawning `transit mcp`, completing an MCP handshake over its stdio, calling a
// tool and killing the child — to reach the socket we can dial directly. It
// would also put a subprocess back on the delivery path, which the herdr sink
// deliberately removed so that a body carrying `$(...)`, quotes, backticks or
// newlines travels as one literal JSON string.
//
// WHAT WE DO NOT DO: render a `transit/1` envelope. That envelope — attribute
// order, escaping, the 4,000-rune clip with `truncated="1"`, the channel
// preview rules — is produced daemon-side by RenderEnvelope
// (transit/daemon/envelope.go) at delivery time and is golden-vector tested
// there. everloop hands Transit a *body* and lets it wrap. everloop's own
// `<channel source="everloop" ...>` envelope becomes that body, so the meta
// contract the MCP server's instructions promise the agent is unchanged: on
// this transport a channel envelope simply arrives inside a transit one.
//
// IDENTITY. Transit derives the sender from the caller, never from the request
// (transit/daemon/ipc.go sendResponse -> callerAgent): a herdr pane id, or the
// pid whose process ancestry reaches a registered native adapter. `everloop
// serve` is a child of the agent's harness process, which is exactly the
// ancestry the Transit MCP server relies on, so the pid resolves with
// herdr.service stopped. HERDR_PANE_ID is sent too when present — an optional
// hint the daemon may use, not a dependency.

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"time"
)

const (
	// transitSocketIOTimeout is the fallback bound when the sink passes none.
	transitSocketIOTimeout = 45 * time.Second

	// transitMaxBodyBytes mirrors maxMessageBytes in transit/daemon/wire.go.
	// A larger body is refused with `body_too_large`, which is permanent: the
	// drain loop would leave the message claimed and retry it every poll
	// forever, and because drainLoop preserves order it would block every
	// message behind it. everloop's own event bound is already 64 KiB of
	// content (maxEventBytes) before headers and this wrapper, so a chatty
	// watch loop crosses the line as a matter of course.
	transitMaxBodyBytes = 64 * 1024

	// transitClipMargin covers clip()'s truncation marker, whose length varies
	// with the digit count of the dropped byte count: 45 bytes of fixed text
	// plus at most 20 digits.
	transitClipMargin = 80
)

// transitSendResult separates "Transit took custody" from "why not". Code is
// set only when the daemon answered with a structured failure; a bare transport
// error leaves it empty, and that difference is what tells a misconfigured
// target apart from a daemon that is not running.
type transitSendResult struct {
	OK bool
	// State is the daemon's custody report: `injected` (handed to the local
	// adapter), `committed` (the Worker acknowledged it) or `spooled` (queued
	// in Transit's own store, which will retry it).
	State   string
	ID      string
	Warning string
	Code    string
	Error   string
}

// transitDriver is the surface the sink needs. It exists so a test can
// substitute a driver without a socket, and so the sink never learns the wire
// format.
type transitDriver interface {
	send(ctx context.Context, to, body string, timeout time.Duration) transitSendResult
}

type transitSocketOptions struct {
	SocketPath string
	Log        func(string)
}

// transitSocketDriver speaks one JSON line per connection, which is what the
// daemon serves: serveIPCConnection reads a single request line, answers, and
// closes.
type transitSocketDriver struct {
	path string
	log  func(string)
}

var _ transitDriver = (*transitSocketDriver)(nil)

// newTransitSocketDriver resolves configuration without dialing. A sink is
// constructed at serve startup, when the Transit daemon may not have come up
// yet — and on the workspace image it is supervised separately — so a missing
// socket then must not stop the MCP tools from coming up. The first delivery
// connects; so does every one after it.
func newTransitSocketDriver(opts transitSocketOptions) (*transitSocketDriver, error) {
	path, err := resolveTransitSocketPath(opts)
	if err != nil {
		return nil, err
	}
	logf := opts.Log
	if logf == nil {
		// stderr, via the standard logger: stdout is the MCP JSON-RPC channel
		// and a stray line there corrupts the session.
		logf = func(message string) { log.Print(message) }
	}
	return &transitSocketDriver{path: path, log: logf}, nil
}

func (d *transitSocketDriver) socketPath() string { return d.path }

// resolveTransitSocketPath mirrors transit's own precedence (dataDir() and
// socketPath() in transit/daemon/config.go), plus one explicit override so a
// test — or a second daemon — does not have to move TRANSIT_DATA_DIR.
func resolveTransitSocketPath(opts transitSocketOptions) (string, error) {
	if opts.SocketPath != "" {
		return opts.SocketPath, nil
	}
	if path := os.Getenv("TRANSIT_SOCKET"); path != "" {
		return path, nil
	}
	if root := os.Getenv("TRANSIT_DATA_DIR"); root != "" {
		return filepath.Join(root, "transit.sock"), nil
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("locate the Transit data directory: %w", err)
	}
	return filepath.Join(home, ".local", "share", "transit", "transit.sock"), nil
}

// transitIPCResponse is the daemon's answer. Every field is optional except
// `ok`; `state` and `warning` only appear on success, `code` and `error` only
// on failure.
type transitIPCResponse struct {
	OK      bool   `json:"ok"`
	ID      string `json:"id"`
	State   string `json:"state"`
	Warning string `json:"warning"`
	Code    string `json:"code"`
	Error   string `json:"error"`
}

func (d *transitSocketDriver) send(ctx context.Context, to, body string, timeout time.Duration) transitSendResult {
	if timeout <= 0 {
		timeout = transitSocketIOTimeout
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	var dialer net.Dialer
	conn, err := dialer.DialContext(ctx, "unix", d.path)
	if err != nil {
		return transitSendResult{Error: fmt.Sprintf("transit daemon is not reachable (socket %s): %v", d.path, err)}
	}
	defer conn.Close()

	if deadline, ok := ctx.Deadline(); ok {
		if err := conn.SetDeadline(deadline); err != nil {
			return transitSendResult{Error: err.Error()}
		}
	}
	stop := context.AfterFunc(ctx, func() { _ = conn.SetDeadline(time.Now()) })
	defer stop()

	request := map[string]any{
		"op":   "send",
		"to":   to,
		"body": body,
		// The daemon resolves the sender from these two and never from the
		// request body — see the IDENTITY note at the top of this file.
		"pid": os.Getpid(),
	}
	if pane := os.Getenv("HERDR_PANE_ID"); pane != "" {
		request["pane_id"] = pane
	}
	if err := json.NewEncoder(conn).Encode(request); err != nil {
		return transitSendResult{Error: fmt.Sprintf("write to %s: %v", d.path, err)}
	}
	var response transitIPCResponse
	if err := json.NewDecoder(conn).Decode(&response); err != nil {
		return transitSendResult{Error: fmt.Sprintf("read from %s: %v", d.path, err)}
	}
	if !response.OK {
		message := response.Error
		if message == "" {
			message = "daemon refused the send"
		}
		return transitSendResult{Code: response.Code, Error: message}
	}
	return transitSendResult{OK: true, State: response.State, ID: response.ID, Warning: response.Warning}
}

// transitBody renders the channel envelope and keeps it inside Transit's store
// bound. The clip lands on the *content*, never on the rendered envelope:
// cutting the wrapper would hand the agent an unterminated tag. clip() is
// everloop's existing truncation — the same one maxRunBytes and maxEventBytes
// use — so a dropped tail stays visible in the body rather than vanishing.
func transitBody(source, content string, meta map[string]string) string {
	body := envelope(source, content, meta)
	if len(body) <= transitMaxBodyBytes {
		return body
	}
	budget := len(content) - (len(body) - transitMaxBodyBytes) - transitClipMargin
	if budget < 0 {
		budget = 0
	}
	return envelope(source, clip(content, budget), meta)
}
