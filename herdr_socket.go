package main

// herdr's socket API — the one transport a drained message travels on.
//
// This speaks herdr's newline-delimited JSON directly instead of exec'ing
// `herdr agent prompt`, which is what this sink used to do. The CLI worked, and
// the reason to stop is not tidiness:
//
//   - A non-zero exit says only "non-zero". The socket returns a structured
//     `error.code`, so `agent_not_found` (the configured recipient is absent —
//     a configuration fault) stays distinguishable from a transport failure
//     (delivery outcome unknown). Collapsing those two is how a queue ends up
//     acked into a void.
//   - `agent_prompt_stalled` is recoverable, and recovering it needs three more
//     calls with a proof step in between (below). Shelling out once per
//     delivery cannot do that.
//   - One process spawn per tick, with the envelope on the argv of a child,
//     for a socket that is already open to us.
//
// Adapted from courier's driver (~/projects/ocai/courier/herdr.go), which is the
// deployed reference for this protocol. It is copied rather than imported
// because courier is `package main`, so nothing in it is importable; and
// because this sink deliberately differs from it in what it accepts and how
// long it waits.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	// everloopHerdrProtocol is what titan's running herdr 0.8.0 reports.
	everloopHerdrProtocol = 19

	// herdrProtocol20 is what the herdr checkout's own API schema declares for
	// its next build. Both are accepted by default, on purpose: this binary is
	// installed on titan and on every agent box, so refusing 20 would take
	// every loop on the fleet down on the day herdr rolls forward, for a
	// protocol we already know about. A version outside the set still refuses
	// rather than guessing — see EVERLOOP_HERDR_PROTOCOL_ALLOW.
	herdrProtocol20 = 20

	herdrSocketIOTimeout = 30 * time.Second

	// herdrCallGrace bounds the call past herdr's own wait deadline, so a
	// server that never answers cannot hold the single-threaded drain loop
	// open forever. The inner deadline is the real one; this is the cover for
	// it not firing.
	herdrCallGrace = 30 * time.Second
)

// herdrPromptResult separates "submitted" from "why not". Code is set only when
// herdr answered with a structured error; a bare transport failure leaves it
// empty, and that difference is load-bearing for the caller's log.
type herdrPromptResult struct {
	OK      bool
	Blocked bool
	Code    string
	Error   string
}

// herdrDriver is the surface the sink needs. It exists so a test can substitute
// a driver without a socket, and so the sink never learns the wire format.
type herdrDriver interface {
	promptAgent(ctx context.Context, target, text string, timeout time.Duration) herdrPromptResult
}

// herdrAgent carries only the fields this transport acts on. pane_id and
// state_change_seq are what the stall recovery needs; agent_status is what
// distinguishes a settled-but-blocked agent from a settled one.
type herdrAgent struct {
	Name           string  `json:"name"`
	Status         string  `json:"agent_status"`
	PaneID         string  `json:"pane_id"`
	StateChangeSeq *uint64 `json:"state_change_seq"`
}

type herdrWireRequest struct {
	ID     string `json:"id"`
	Method string `json:"method"`
	Params any    `json:"params"`
}

type herdrWireResponse struct {
	ID     string          `json:"id"`
	Result json.RawMessage `json:"result"`
	Error  *herdrAPIError  `json:"error"`
}

type herdrAPIError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func (e *herdrAPIError) Error() string {
	if e.Message != "" {
		return e.Message
	}
	if e.Code != "" {
		return e.Code
	}
	return "unknown herdr API error"
}

type herdrPingResult struct {
	Type     string `json:"type"`
	Version  string `json:"version"`
	Protocol int    `json:"protocol"`
}

type herdrSocketOptions struct {
	// SocketPath is the low-level override; Session is the equivalent of an
	// explicit --session and applies only when SocketPath is empty.
	SocketPath string
	Session    string
	Log        func(string)
}

// herdrSocketDriver serializes calls so request ids stay deterministic. herdr
// serves exactly one request per accepted connection, so the protocol check and
// the operation each get their own socket.
type herdrSocketDriver struct {
	path     string
	accepted map[int]struct{}
	log      func(string)

	mu             sync.Mutex
	nextID         uint64
	loggedProtocol int

	stallPollInterval time.Duration
	stallPollAttempts int
}

var _ herdrDriver = (*herdrSocketDriver)(nil)

// newHerdrSocketDriver resolves configuration without dialing: a sink is
// constructed at serve startup, when herdr may not have answered yet, and a
// missing socket then must not stop the MCP tools from coming up. The first
// delivery connects and verifies the protocol; so does every reconnect.
func newHerdrSocketDriver(opts herdrSocketOptions) (*herdrSocketDriver, error) {
	path, err := resolveHerdrSocketPath(opts)
	if err != nil {
		return nil, err
	}
	logf := opts.Log
	if logf == nil {
		// stderr, via the standard logger: stdout is the MCP JSON-RPC channel
		// and a stray line there corrupts the session.
		logf = func(message string) { log.Print(message) }
	}
	return &herdrSocketDriver{
		path:              path,
		accepted:          acceptedHerdrProtocols(os.Getenv("EVERLOOP_HERDR_PROTOCOL_ALLOW")),
		log:               logf,
		stallPollInterval: time.Second,
		stallPollAttempts: 15,
	}, nil
}

func (d *herdrSocketDriver) socketPath() string { return d.path }

// resolveHerdrSocketPath mirrors the CLI's own precedence, which is what the
// exec-based predecessor got for free by inheriting the environment.
func resolveHerdrSocketPath(opts herdrSocketOptions) (string, error) {
	if opts.SocketPath != "" {
		return opts.SocketPath, nil
	}
	if opts.Session != "" {
		return herdrSocketPathForSession(opts.Session)
	}
	if path := os.Getenv("HERDR_SOCKET_PATH"); path != "" {
		return path, nil
	}
	if session := os.Getenv("HERDR_SESSION"); session != "" {
		return herdrSocketPathForSession(session)
	}
	return herdrSocketPathForSession("")
}

// acceptedHerdrProtocols never fails on a malformed entry: this is read at
// startup on eight boxes, and a typo in an override must not stop delivery for
// a protocol that would have been accepted anyway. Unparseable tokens are
// skipped rather than treated as a fatal misconfiguration.
func acceptedHerdrProtocols(extra string) map[int]struct{} {
	accepted := map[int]struct{}{everloopHerdrProtocol: {}, herdrProtocol20: {}}
	for _, token := range strings.Split(extra, ",") {
		token = strings.TrimSpace(token)
		if token == "" {
			continue
		}
		if version, err := strconv.Atoi(token); err == nil && version >= 0 {
			accepted[version] = struct{}{}
		}
	}
	return accepted
}

func sortedHerdrProtocols(accepted map[int]struct{}) []int {
	versions := make([]int, 0, len(accepted))
	for version := range accepted {
		versions = append(versions, version)
	}
	sort.Ints(versions)
	return versions
}

func herdrSocketPathForSession(session string) (string, error) {
	if session == "default" {
		session = ""
	}
	if session != "" {
		if len(session) > 64 {
			return "", errors.New("herdr session name cannot be longer than 64 bytes")
		}
		if session == "." || session == ".." {
			return "", errors.New("herdr session name cannot be . or ..")
		}
		for _, b := range session {
			if !((b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') || (b >= '0' && b <= '9') || b == '.' || b == '_' || b == '-') {
				return "", errors.New("herdr session name may only contain ASCII letters, numbers, '.', '_' and '-'")
			}
		}
	}

	configHome := os.Getenv("XDG_CONFIG_HOME")
	if configHome == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return "", fmt.Errorf("resolve herdr config directory: %w", err)
		}
		configHome = filepath.Join(home, ".config")
	}
	base := filepath.Join(configHome, "herdr")
	if session != "" {
		base = filepath.Join(base, "sessions", session)
	}
	return filepath.Join(base, "herdr.sock"), nil
}

func (d *herdrSocketDriver) dial(ctx context.Context) (net.Conn, *json.Encoder, *json.Decoder, error) {
	var dialer net.Dialer
	conn, err := dialer.DialContext(ctx, "unix", d.path)
	if err != nil {
		return nil, nil, nil, err
	}
	return conn, json.NewEncoder(conn), json.NewDecoder(conn), nil
}

// verifyProtocolLocked pings on its own connection and refuses an unaccepted
// protocol *before* any operation is sent, so an incompatible server never
// receives a prompt it might interpret differently.
func (d *herdrSocketDriver) verifyProtocolLocked(ctx context.Context) error {
	conn, encoder, decoder, err := d.dial(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()

	response, err := d.exchange(ctx, conn, encoder, decoder, "ping", struct{}{})
	if err != nil {
		return err
	}
	if response.Error != nil {
		return response.Error
	}
	var pong herdrPingResult
	if len(response.Result) == 0 || json.Unmarshal(response.Result, &pong) != nil || pong.Type != "pong" {
		return errors.New("ping returned an unrecognized response")
	}
	if _, ok := d.accepted[pong.Protocol]; !ok {
		return fmt.Errorf("herdr protocol %d is not accepted (accepted: %v)", pong.Protocol, sortedHerdrProtocols(d.accepted))
	}
	if d.loggedProtocol != pong.Protocol {
		d.log(fmt.Sprintf("everloop: herdr protocol %d accepted at %s", pong.Protocol, d.path))
		d.loggedProtocol = pong.Protocol
	}
	return nil
}

func (d *herdrSocketDriver) exchange(ctx context.Context, conn net.Conn, encoder *json.Encoder, decoder *json.Decoder, method string, params any) (herdrWireResponse, error) {
	d.nextID++
	id := fmt.Sprintf("req_%d", d.nextID)
	if deadline, ok := ctx.Deadline(); ok {
		if err := conn.SetDeadline(deadline); err != nil {
			return herdrWireResponse{}, err
		}
	}
	stop := context.AfterFunc(ctx, func() { _ = conn.SetDeadline(time.Now()) })
	defer func() {
		if stop() {
			_ = conn.SetDeadline(time.Time{})
		}
	}()

	if err := encoder.Encode(herdrWireRequest{ID: id, Method: method, Params: params}); err != nil {
		return herdrWireResponse{}, err
	}
	var response herdrWireResponse
	if err := decoder.Decode(&response); err != nil {
		return herdrWireResponse{}, err
	}
	if err := ctx.Err(); err != nil {
		return herdrWireResponse{}, err
	}
	// An id mismatch means the stream is not the answer to this question.
	if response.ID != id {
		return herdrWireResponse{}, fmt.Errorf("response id %q does not match request id %q", response.ID, id)
	}
	if response.Error == nil && len(response.Result) == 0 {
		return herdrWireResponse{}, errors.New("response contains neither result nor error")
	}
	return response, nil
}

func (d *herdrSocketDriver) call(ctx context.Context, method string, params any, bound time.Duration) (json.RawMessage, error) {
	if bound <= 0 {
		bound = herdrSocketIOTimeout
	}
	callCtx, cancel := context.WithTimeout(ctx, bound)
	defer cancel()

	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.verifyProtocolLocked(callCtx); err != nil {
		return nil, err
	}
	conn, encoder, decoder, err := d.dial(callCtx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	response, err := d.exchange(callCtx, conn, encoder, decoder, method, params)
	if err != nil {
		return nil, err
	}
	if response.Error != nil {
		return nil, response.Error
	}
	return response.Result, nil
}

func herdrTimeoutWithGrace(timeout time.Duration) time.Duration {
	if timeout > time.Duration(math.MaxInt64)-herdrCallGrace {
		return timeout
	}
	return timeout + herdrCallGrace
}

func herdrDurationMillis(timeout time.Duration) int64 {
	ms := timeout.Milliseconds()
	if ms < 1 {
		return 1
	}
	return ms
}

type herdrAgentResponse struct {
	Type  string      `json:"type"`
	Agent *herdrAgent `json:"agent"`
}

func decodeHerdrAgent(raw json.RawMessage, allowedTypes ...string) (*herdrAgent, error) {
	var response herdrAgentResponse
	if err := json.Unmarshal(raw, &response); err != nil {
		return nil, err
	}
	for _, want := range allowedTypes {
		if response.Type == want && response.Agent != nil {
			return response.Agent, nil
		}
	}
	return nil, fmt.Errorf("unexpected %q response without an agent", response.Type)
}

// herdrPromptFailure keeps herdr's own error code on the result. The caller
// puts it in the message it returns, which is the only place an operator sees
// the difference between "no such agent" and "could not reach herdr".
func herdrPromptFailure(err error) herdrPromptResult {
	var apiErr *herdrAPIError
	if errors.As(err, &apiErr) {
		return herdrPromptResult{Code: apiErr.Code, Error: apiErr.Error()}
	}
	return herdrPromptResult{Error: err.Error()}
}

// promptAgent submits one envelope and waits for the agent to settle. `blocked`
// counts as settled: the prompt was consumed and the agent stopped on a
// permission prompt, so the tick was delivered even though no answer came back.
func (d *herdrSocketDriver) promptAgent(ctx context.Context, target, text string, timeout time.Duration) herdrPromptResult {
	if timeout <= 0 {
		timeout = time.Duration(herdrDefaultTimeoutMS) * time.Millisecond
	}
	callCtx, cancel := context.WithTimeout(ctx, herdrTimeoutWithGrace(timeout))
	defer cancel()

	params := map[string]any{
		"target": target,
		"text":   text,
		"wait": map[string]any{
			"until":      []string{"idle", "done", "blocked"},
			"timeout_ms": herdrDurationMillis(timeout),
		},
	}
	raw, err := d.call(callCtx, "agent.prompt", params, herdrTimeoutWithGrace(timeout))
	if err != nil {
		var apiErr *herdrAPIError
		if errors.As(err, &apiErr) && apiErr.Code == "agent_prompt_stalled" {
			if recovered, definitive := d.flushPastedPrompt(callCtx, target, timeout); definitive {
				return recovered
			}
		}
		return herdrPromptFailure(err)
	}
	agent, err := decodeHerdrAgent(raw, "agent_prompted")
	if err != nil {
		return herdrPromptResult{Error: fmt.Sprintf("herdr agent prompt %s: %v", target, err)}
	}
	return herdrPromptResult{OK: true, Blocked: agent.Status == "blocked"}
}

// flushPastedPrompt recovers one measured failure: a large bracketed paste can
// collapse into an omp attachment chip and absorb herdr's submit key, so the
// prompt sits unsent in the composer and herdr answers agent_prompt_stalled for
// a message that did arrive. One Enter is safe on an empty input, but accepting
// the keypress proves nothing — state_change_seq must MOVE before this waits or
// reports success. When it cannot be proven, definitive=false and the caller
// reports the original stall rather than a success it did not observe.
func (d *herdrSocketDriver) flushPastedPrompt(ctx context.Context, target string, timeout time.Duration) (herdrPromptResult, bool) {
	agent, err := d.getAgent(ctx, target)
	if err != nil || agent == nil || agent.PaneID == "" {
		return herdrPromptResult{}, false
	}
	before := agent.StateChangeSeq
	if !d.sendKeys(ctx, agent.PaneID, []string{"Enter"}) {
		return herdrPromptResult{}, false
	}

	moved := false
	for range d.stallPollAttempts {
		timer := time.NewTimer(d.stallPollInterval)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return herdrPromptResult{}, false
		case <-timer.C:
		}
		now, err := d.getAgent(ctx, target)
		if err != nil || now == nil {
			return herdrPromptResult{}, false
		}
		if before == nil || now.StateChangeSeq == nil || *now.StateChangeSeq != *before {
			moved = true
			break
		}
	}
	if !moved {
		return herdrPromptResult{}, false
	}

	params := map[string]any{
		"target":     target,
		"until":      []string{"idle", "done", "blocked"},
		"timeout_ms": herdrDurationMillis(timeout),
	}
	raw, err := d.call(ctx, "agent.wait", params, herdrTimeoutWithGrace(timeout))
	if err != nil {
		return herdrPromptFailure(err), true
	}
	settled, err := decodeHerdrAgent(raw, "agent_info")
	if err != nil {
		return herdrPromptResult{Error: fmt.Sprintf("herdr agent wait %s: %v", target, err)}, true
	}
	return herdrPromptResult{OK: true, Blocked: settled.Status == "blocked"}, true
}

// getAgent returns (nil, nil) for agent_not_found — an intentional absence —
// and an error for anything else. An unreachable socket is NOT evidence that
// the agent is gone, and the recovery above only runs when a pane is known.
func (d *herdrSocketDriver) getAgent(ctx context.Context, target string) (*herdrAgent, error) {
	raw, err := d.call(ctx, "agent.get", map[string]any{"target": target}, herdrSocketIOTimeout)
	if err != nil {
		var apiErr *herdrAPIError
		if errors.As(err, &apiErr) && apiErr.Code == "agent_not_found" {
			return nil, nil
		}
		return nil, fmt.Errorf("herdr agent get %s: %w", target, err)
	}
	agent, err := decodeHerdrAgent(raw, "agent_info")
	if err != nil {
		return nil, fmt.Errorf("herdr agent get %s: %w", target, err)
	}
	return agent, nil
}

func (d *herdrSocketDriver) sendKeys(ctx context.Context, paneID string, keys []string) bool {
	_, err := d.call(ctx, "pane.send_keys", map[string]any{"pane_id": paneID, "keys": keys}, herdrSocketIOTimeout)
	return err == nil
}
