package main

// Sink tests: envelope rendering, and the herdr transport against a fake herdr
// SERVER on a unix socket.
//
// The predecessor of this file tested a fake herdr *binary* — a shell script on
// PATH that recorded its argv. That could only ever assert the shape of a
// command line. These tests assert the things that actually decide whether a
// tick is delivered: the protocol handshake, the exact agent.prompt params, the
// difference between "no such agent" and "could not reach herdr", and the
// stalled-paste recovery, including the branch where recovery cannot prove the
// prompt was submitted and must not claim success.

import (
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

type fakeReply struct {
	result   any
	apiError *herdrAPIError
}

type fakeHerdrServer struct {
	listener net.Listener
	done     chan struct{}
	wg       sync.WaitGroup

	mu      sync.Mutex
	methods []string
	params  []map[string]any
}

func startFakeHerdr(t *testing.T, protocol int, handler func(method string, params map[string]any) fakeReply) *fakeHerdrServer {
	t.Helper()
	// A unix socket path is bounded (~108 bytes) and t.TempDir() with a long
	// test name can exceed it, so keep the leaf short.
	path := filepath.Join(t.TempDir(), "h.sock")
	listener, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	server := &fakeHerdrServer{listener: listener, done: make(chan struct{})}
	server.wg.Add(1)
	go func() {
		defer server.wg.Done()
		for {
			conn, err := listener.Accept()
			if err != nil {
				select {
				case <-server.done:
					return
				default:
					return
				}
			}
			server.wg.Add(1)
			go server.serveConn(conn, protocol, handler)
		}
	}()
	t.Cleanup(func() {
		close(server.done)
		_ = listener.Close()
		server.wg.Wait()
	})
	return server
}

// serveConn answers exactly one request per connection, which is what herdr
// itself does — and is why the driver dials twice per call.
func (s *fakeHerdrServer) serveConn(conn net.Conn, protocol int, handler func(string, map[string]any) fakeReply) {
	defer s.wg.Done()
	defer conn.Close()
	decoder := json.NewDecoder(conn)
	encoder := json.NewEncoder(conn)
	var request struct {
		ID     string         `json:"id"`
		Method string         `json:"method"`
		Params map[string]any `json:"params"`
	}
	if err := decoder.Decode(&request); err != nil {
		return
	}
	if request.Method == "ping" {
		raw, _ := json.Marshal(map[string]any{"type": "pong", "version": "0.8.0", "protocol": protocol})
		_ = encoder.Encode(herdrWireResponse{ID: request.ID, Result: raw})
		return
	}

	s.mu.Lock()
	s.methods = append(s.methods, request.Method)
	s.params = append(s.params, request.Params)
	s.mu.Unlock()

	reply := handler(request.Method, request.Params)
	response := herdrWireResponse{ID: request.ID, Error: reply.apiError}
	if reply.apiError == nil {
		raw, _ := json.Marshal(reply.result)
		response.Result = raw
	}
	_ = encoder.Encode(response)
}

func (s *fakeHerdrServer) calls() ([]string, []map[string]any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.methods...), append([]map[string]any(nil), s.params...)
}

func (s *fakeHerdrServer) path() string { return s.listener.Addr().String() }

func agentInfo(kind, status string, seq *uint64) map[string]any {
	agent := map[string]any{"name": "jessica", "agent_status": status, "pane_id": "w7N:p5"}
	if seq != nil {
		agent["state_change_seq"] = *seq
	}
	return map[string]any{"type": kind, "agent": agent}
}

func seqPtr(v uint64) *uint64 { return &v }

func testDriver(t *testing.T, server *fakeHerdrServer) *herdrSocketDriver {
	t.Helper()
	driver, err := newHerdrSocketDriver(herdrSocketOptions{
		SocketPath: server.path(),
		Log:        func(string) {},
	})
	if err != nil {
		t.Fatal(err)
	}
	// The recovery path is exercised for real, so keep its polling short.
	driver.stallPollInterval = 5 * time.Millisecond
	driver.stallPollAttempts = 3
	return driver
}

func TestEnvelopeEscapesAndSortsAttrs(t *testing.T) {
	got := envelope("tincan", "hello\nworld", map[string]string{
		"from":  `a&b"c`,
		"kind":  "message",
		"empty": "",
	})
	want := "<channel source=\"tincan\" from=\"a&amp;b&quot;c\" kind=\"message\">\nhello\nworld\n</channel>"
	if got != want {
		t.Fatalf("envelope mismatch:\n got: %q\nwant: %q", got, want)
	}
}

func TestHerdrSinkSendsEnvelopeAndWaitParams(t *testing.T) {
	server := startFakeHerdr(t, everloopHerdrProtocol, func(method string, _ map[string]any) fakeReply {
		if method != "agent.prompt" {
			t.Errorf("unexpected method %q", method)
		}
		return fakeReply{result: agentInfo("agent_prompted", "idle", nil)}
	})
	s := &herdrSink{driver: testDriver(t, server), target: "jessica", timeout: 90 * time.Second, source: "everloop"}

	if err := s.deliver("reconcile the ledger", map[string]string{"kind": "tick", "loop": "recon"}); err != nil {
		t.Fatal(err)
	}

	methods, params := server.calls()
	if len(methods) != 1 || methods[0] != "agent.prompt" {
		t.Fatalf("expected one agent.prompt, got %v", methods)
	}
	if params[0]["target"] != "jessica" {
		t.Fatalf("target = %v", params[0]["target"])
	}
	text, _ := params[0]["text"].(string)
	if !strings.HasPrefix(text, `<channel source="everloop" kind="tick" loop="recon">`) ||
		!strings.Contains(text, "reconcile the ledger") {
		t.Fatalf("envelope not sent verbatim: %q", text)
	}
	wait, ok := params[0]["wait"].(map[string]any)
	if !ok {
		t.Fatalf("no wait block: %v", params[0])
	}
	if ms, _ := wait["timeout_ms"].(float64); int64(ms) != 90000 {
		t.Fatalf("timeout_ms = %v, want 90000", wait["timeout_ms"])
	}
	until, _ := wait["until"].([]any)
	if len(until) != 3 || until[0] != "idle" || until[1] != "done" || until[2] != "blocked" {
		t.Fatalf("until = %v", wait["until"])
	}
}

// A settled-but-blocked agent consumed the prompt, so the tick is delivered and
// the spool entry is acked. Treating it as a failure would redeliver the same
// tick every poll for as long as the agent sat on its permission prompt.
func TestHerdrSinkAcksWhenAgentIsBlocked(t *testing.T) {
	server := startFakeHerdr(t, everloopHerdrProtocol, func(string, map[string]any) fakeReply {
		return fakeReply{result: agentInfo("agent_prompted", "blocked", nil)}
	})
	s := &herdrSink{driver: testDriver(t, server), target: "jessica", timeout: time.Minute, source: "everloop"}
	if err := s.deliver("tick", nil); err != nil {
		t.Fatalf("blocked must still ack: %v", err)
	}
}

// agent_not_found is a configuration fault and must be legible as one: the code
// travels into the returned error so an operator sees which of the two failures
// happened without correlating logs.
func TestHerdrSinkSurfacesAgentNotFoundCode(t *testing.T) {
	server := startFakeHerdr(t, everloopHerdrProtocol, func(string, map[string]any) fakeReply {
		return fakeReply{apiError: &herdrAPIError{Code: "agent_not_found", Message: "no agent named nobody"}}
	})
	s := &herdrSink{driver: testDriver(t, server), target: "nobody", timeout: time.Minute, source: "everloop"}
	err := s.deliver("tick", nil)
	if err == nil {
		t.Fatal("expected an error")
	}
	if !strings.Contains(err.Error(), "agent_not_found") {
		t.Fatalf("error must name the code, got %q", err)
	}
}

// A transport failure carries no code — the distinction the CLI could not make.
func TestHerdrSinkTransportFailureHasNoCode(t *testing.T) {
	s := &herdrSink{
		driver:  &herdrSocketDriver{path: filepath.Join(t.TempDir(), "absent.sock"), accepted: acceptedHerdrProtocols(""), log: func(string) {}},
		target:  "jessica",
		timeout: time.Second,
		source:  "everloop",
	}
	err := s.deliver("tick", nil)
	if err == nil {
		t.Fatal("expected an error")
	}
	for _, code := range []string{"agent_not_found", "agent_prompt_stalled"} {
		if strings.Contains(err.Error(), code) {
			t.Fatalf("transport failure must not be labelled %s: %q", code, err)
		}
	}
}

func TestHerdrDriverRefusesUnacceptedProtocol(t *testing.T) {
	server := startFakeHerdr(t, 3, func(method string, _ map[string]any) fakeReply {
		t.Errorf("no operation may be sent to an unaccepted server, got %q", method)
		return fakeReply{result: agentInfo("agent_prompted", "idle", nil)}
	})
	s := &herdrSink{driver: testDriver(t, server), target: "jessica", timeout: time.Second, source: "everloop"}
	err := s.deliver("tick", nil)
	if err == nil {
		t.Fatal("expected a protocol refusal")
	}
	if !strings.Contains(err.Error(), "protocol 3 is not accepted") || !strings.Contains(err.Error(), "19") {
		t.Fatalf("refusal must name the server and accepted versions, got %q", err)
	}
	if methods, _ := server.calls(); len(methods) != 0 {
		t.Fatalf("operations leaked to an unaccepted server: %v", methods)
	}
}

func TestHerdrProtocolAllowWidensAcceptance(t *testing.T) {
	server := startFakeHerdr(t, 21, func(string, map[string]any) fakeReply {
		return fakeReply{result: agentInfo("agent_prompted", "idle", nil)}
	})
	driver, err := newHerdrSocketDriver(herdrSocketOptions{SocketPath: server.path(), Log: func(string) {}})
	if err != nil {
		t.Fatal(err)
	}
	driver.accepted = acceptedHerdrProtocols("21, bogus,")
	s := &herdrSink{driver: driver, target: "jessica", timeout: time.Second, source: "everloop"}
	if err := s.deliver("tick", nil); err != nil {
		t.Fatalf("21 was allow-listed: %v", err)
	}
	if _, ok := acceptedHerdrProtocols("bogus")[21]; ok {
		t.Fatal("an unparseable token must not add a version")
	}
	// The default set is both known protocols, so a herdr roll does not need an
	// override at all.
	for _, want := range []int{19, 20} {
		if _, ok := acceptedHerdrProtocols("")[want]; !ok {
			t.Fatalf("protocol %d must be accepted by default", want)
		}
	}
}

// The stall is recoverable only when the pane's state_change_seq MOVES after the
// Enter. This asserts the whole sequence, in order.
func TestHerdrDriverRecoversStalledPaste(t *testing.T) {
	seq := uint64(41)
	server := startFakeHerdr(t, everloopHerdrProtocol, func(method string, _ map[string]any) fakeReply {
		switch method {
		case "agent.prompt":
			return fakeReply{apiError: &herdrAPIError{Code: "agent_prompt_stalled", Message: "no submission observed"}}
		case "agent.get":
			current := seq
			seq++ // the pane advances once the Enter lands
			return fakeReply{result: agentInfo("agent_info", "working", seqPtr(current))}
		case "pane.send_keys":
			return fakeReply{result: map[string]any{"type": "keys_sent"}}
		case "agent.wait":
			return fakeReply{result: agentInfo("agent_info", "idle", seqPtr(99))}
		}
		t.Errorf("unexpected method %q", method)
		return fakeReply{result: map[string]any{}}
	})
	s := &herdrSink{driver: testDriver(t, server), target: "jessica", timeout: 2 * time.Second, source: "everloop"}
	if err := s.deliver("a very long tick", nil); err != nil {
		t.Fatalf("recovery should have acked: %v", err)
	}
	methods, _ := server.calls()
	joined := strings.Join(methods, ",")
	for _, want := range []string{"agent.prompt", "agent.get", "pane.send_keys", "agent.wait"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("recovery skipped %s: %v", want, methods)
		}
	}
	if strings.Index(joined, "pane.send_keys") > strings.Index(joined, "agent.wait") {
		t.Fatalf("waited before pressing Enter: %v", methods)
	}
}

// If the sequence never moves, the Enter proved nothing: the original stall must
// be reported rather than a success nobody observed.
func TestHerdrDriverReportsStallWhenSeqNeverMoves(t *testing.T) {
	server := startFakeHerdr(t, everloopHerdrProtocol, func(method string, _ map[string]any) fakeReply {
		switch method {
		case "agent.prompt":
			return fakeReply{apiError: &herdrAPIError{Code: "agent_prompt_stalled", Message: "no submission observed"}}
		case "agent.get":
			return fakeReply{result: agentInfo("agent_info", "idle", seqPtr(7))} // frozen
		case "pane.send_keys":
			return fakeReply{result: map[string]any{"type": "keys_sent"}}
		case "agent.wait":
			t.Error("agent.wait must not run when the sequence never moved")
		}
		return fakeReply{result: map[string]any{}}
	})
	s := &herdrSink{driver: testDriver(t, server), target: "jessica", timeout: time.Second, source: "everloop"}
	err := s.deliver("a very long tick", nil)
	if err == nil {
		t.Fatal("expected the original stall to be reported")
	}
	if !strings.Contains(err.Error(), "agent_prompt_stalled") {
		t.Fatalf("must report the original stall, got %q", err)
	}
}

func TestHerdrSocketPathPrecedence(t *testing.T) {
	t.Setenv("XDG_CONFIG_HOME", "/xdg")
	t.Setenv("HERDR_SOCKET_PATH", "/env/explicit.sock")
	t.Setenv("HERDR_SESSION", "envsession")

	if got, err := resolveHerdrSocketPath(herdrSocketOptions{SocketPath: "/opt/one.sock", Session: "ignored"}); err != nil || got != "/opt/one.sock" {
		t.Fatalf("explicit path must win: %q %v", got, err)
	}
	if got, err := resolveHerdrSocketPath(herdrSocketOptions{Session: "named"}); err != nil || got != "/xdg/herdr/sessions/named/herdr.sock" {
		t.Fatalf("explicit session must beat env: %q %v", got, err)
	}
	if got, err := resolveHerdrSocketPath(herdrSocketOptions{}); err != nil || got != "/env/explicit.sock" {
		t.Fatalf("HERDR_SOCKET_PATH must beat HERDR_SESSION: %q %v", got, err)
	}
	os.Unsetenv("HERDR_SOCKET_PATH")
	if got, err := resolveHerdrSocketPath(herdrSocketOptions{}); err != nil || got != "/xdg/herdr/sessions/envsession/herdr.sock" {
		t.Fatalf("HERDR_SESSION path: %q %v", got, err)
	}
	t.Setenv("HERDR_SESSION", "default")
	if got, err := resolveHerdrSocketPath(herdrSocketOptions{}); err != nil || got != "/xdg/herdr/herdr.sock" {
		t.Fatalf("session 'default' must normalize to the default socket: %q %v", got, err)
	}
}

func TestHerdrDriverRejectsMismatchedResponseID(t *testing.T) {
	path := filepath.Join(t.TempDir(), "h.sock")
	listener, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				decoder := json.NewDecoder(c)
				encoder := json.NewEncoder(c)
				var req struct {
					ID     string `json:"id"`
					Method string `json:"method"`
				}
				if decoder.Decode(&req) != nil {
					return
				}
				if req.Method == "ping" {
					raw, _ := json.Marshal(map[string]any{"type": "pong", "protocol": everloopHerdrProtocol})
					_ = encoder.Encode(herdrWireResponse{ID: req.ID, Result: raw})
					return
				}
				raw, _ := json.Marshal(agentInfo("agent_prompted", "idle", nil))
				_ = encoder.Encode(herdrWireResponse{ID: "someone_elses_request", Result: raw})
			}(conn)
		}
	}()

	s := &herdrSink{
		driver:  &herdrSocketDriver{path: path, accepted: acceptedHerdrProtocols(""), log: func(string) {}},
		target:  "jessica",
		timeout: 2 * time.Second,
		source:  "everloop",
	}
	err = s.deliver("tick", nil)
	if err == nil || !strings.Contains(err.Error(), "does not match request id") {
		t.Fatalf("a mismatched id must be refused, got %v", err)
	}
}

func TestNewSinkDefaultsToHerdrAndRequiresTarget(t *testing.T) {
	t.Setenv("CHANNEL_SINK", "")
	t.Setenv("HERDR_TARGET", "")
	if _, err := newSink("everloop"); err == nil || !strings.Contains(err.Error(), "HERDR_TARGET") {
		t.Fatalf("default sink must refuse without a target, got %v", err)
	}

	t.Setenv("HERDR_TARGET", "jessica")
	dlv, err := newSink("everloop")
	if err != nil {
		t.Fatal(err)
	}
	s, ok := dlv.(*herdrSink)
	if !ok {
		t.Fatalf("default sink is %T, want *herdrSink", dlv)
	}
	if s.target != "jessica" || s.timeout != time.Duration(herdrDefaultTimeoutMS)*time.Millisecond {
		t.Fatalf("target=%q timeout=%v", s.target, s.timeout)
	}
}

func TestNewSinkRejectsRemovedSinks(t *testing.T) {
	t.Setenv("HERDR_TARGET", "jessica")
	for _, removed := range []string{"claude", "opencode", "hermes"} {
		t.Setenv("CHANNEL_SINK", removed)
		if _, err := newSink("everloop"); err == nil {
			t.Fatalf("CHANNEL_SINK=%s must refuse, not fall back to a transport", removed)
		} else if !strings.Contains(err.Error(), "want herdr") {
			t.Fatalf("refusal should name the accepted values, got %q", err)
		}
	}
}

func TestNewSinkNoneIsToolsOnly(t *testing.T) {
	t.Setenv("CHANNEL_SINK", "none")
	t.Setenv("HERDR_TARGET", "")
	dlv, err := newSink("everloop")
	if err != nil || dlv != nil {
		t.Fatalf("none must be a nil sink with no error: %v %v", dlv, err)
	}
}

func TestNewSinkRejectsBadTimeout(t *testing.T) {
	t.Setenv("CHANNEL_SINK", "herdr")
	t.Setenv("HERDR_TARGET", "jessica")
	for _, bad := range []string{"0", "-1", "abc", "12.5"} {
		t.Setenv("HERDR_PROMPT_TIMEOUT_MS", bad)
		if _, err := newSink("everloop"); err == nil {
			t.Fatalf("HERDR_PROMPT_TIMEOUT_MS=%q must be refused", bad)
		}
	}
	t.Setenv("HERDR_PROMPT_TIMEOUT_MS", "45000")
	dlv, err := newSink("everloop")
	if err != nil {
		t.Fatal(err)
	}
	if got := dlv.(*herdrSink).timeout; got != 45*time.Second {
		t.Fatalf("timeout = %v, want 45s", got)
	}
}

// The transport must not depend on a `herdr` executable existing at all: an
// empty PATH changes nothing, which is the whole point of the socket cutover.
func TestDeliveryNeedsNoHerdrBinary(t *testing.T) {
	t.Setenv("PATH", "")
	server := startFakeHerdr(t, everloopHerdrProtocol, func(string, map[string]any) fakeReply {
		return fakeReply{result: agentInfo("agent_prompted", "idle", nil)}
	})
	s := &herdrSink{driver: testDriver(t, server), target: "jessica", timeout: time.Second, source: "everloop"}
	if err := s.deliver("tick", nil); err != nil {
		t.Fatalf("delivery must not need an executable: %v", err)
	}
	if _, err := os.Stat("/nonexistent/herdr"); !errors.Is(err, os.ErrNotExist) {
		t.Skip("sanity check only")
	}
}
