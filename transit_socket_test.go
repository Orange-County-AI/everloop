package main

// Transit sink tests: sink selection, missing-config refusal, and what actually
// goes on the wire — against a fake Transit DAEMON on a unix socket, the same
// way the herdr tests fake herdr. Nothing here needs a live daemon, and nothing
// here asserts the `transit/1` envelope: that envelope is rendered daemon-side
// by RenderEnvelope and golden-vector tested in the Transit repo. What everloop
// owns, and what these tests pin, is the request: op, target, identity fields,
// and the channel envelope that becomes the transit body.

import (
	"encoding/json"
	"net"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

type fakeTransitDaemon struct {
	listener net.Listener
	wg       sync.WaitGroup

	mu       sync.Mutex
	requests []map[string]any
}

// startFakeTransit serves one JSON line per connection and answers with the
// handler's map, which is exactly the daemon's serveIPCConnection contract.
func startFakeTransit(t *testing.T, handler func(request map[string]any) map[string]any) *fakeTransitDaemon {
	t.Helper()
	// A unix socket path is bounded (~108 bytes) and t.TempDir() with a long
	// test name can exceed it, so keep the leaf short.
	path := filepath.Join(t.TempDir(), "t.sock")
	listener, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	daemon := &fakeTransitDaemon{listener: listener}
	daemon.wg.Add(1)
	go func() {
		defer daemon.wg.Done()
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			daemon.wg.Add(1)
			go daemon.serveConn(conn, handler)
		}
	}()
	t.Cleanup(func() {
		_ = listener.Close()
		daemon.wg.Wait()
	})
	return daemon
}

func (d *fakeTransitDaemon) serveConn(conn net.Conn, handler func(map[string]any) map[string]any) {
	defer d.wg.Done()
	defer conn.Close()
	var request map[string]any
	if err := json.NewDecoder(conn).Decode(&request); err != nil {
		return
	}
	d.mu.Lock()
	d.requests = append(d.requests, request)
	d.mu.Unlock()
	_ = json.NewEncoder(conn).Encode(handler(request))
}

func (d *fakeTransitDaemon) calls() []map[string]any {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]map[string]any(nil), d.requests...)
}

func (d *fakeTransitDaemon) path() string { return d.listener.Addr().String() }

func transitTestSink(t *testing.T, daemon *fakeTransitDaemon) *transitSink {
	t.Helper()
	driver, err := newTransitSocketDriver(transitSocketOptions{
		SocketPath: daemon.path(),
		Log:        func(string) {},
	})
	if err != nil {
		t.Fatal(err)
	}
	return &transitSink{driver: driver, target: "jessica@titan", timeout: 5 * time.Second, source: "everloop", log: func(string) {}}
}

func TestTransitSinkSendsChannelEnvelopeAsBody(t *testing.T) {
	t.Setenv("HERDR_PANE_ID", "w7N:p5")
	daemon := startFakeTransit(t, func(map[string]any) map[string]any {
		return map[string]any{"ok": true, "id": "tx_1", "state": "injected", "local": true}
	})
	s := transitTestSink(t, daemon)

	if err := s.deliver("reconcile the ledger", map[string]string{"kind": "tick", "loop": "recon", "coalesced_count": "3"}); err != nil {
		t.Fatal(err)
	}

	calls := daemon.calls()
	if len(calls) != 1 {
		t.Fatalf("made %d calls, want 1", len(calls))
	}
	call := calls[0]
	if call["op"] != "send" {
		t.Fatalf("op = %v, want send", call["op"])
	}
	if call["to"] != "jessica@titan" {
		t.Fatalf("to = %v, want jessica@titan", call["to"])
	}
	// Identity: the daemon derives the sender from these and never from the
	// request body. pid must always be present; pane_id is the optional hint.
	if pid, ok := call["pid"].(float64); !ok || pid <= 0 {
		t.Fatalf("pid = %v, want this process's pid", call["pid"])
	}
	if call["pane_id"] != "w7N:p5" {
		t.Fatalf("pane_id = %v, want the HERDR_PANE_ID hint", call["pane_id"])
	}
	want := envelope("everloop", "reconcile the ledger", map[string]string{"kind": "tick", "loop": "recon", "coalesced_count": "3"})
	if call["body"] != want {
		t.Fatalf("body =\n%v\nwant\n%v", call["body"], want)
	}
	// everloop must not render transit/1 itself; the daemon does that.
	if strings.Contains(want, "schema=\"transit/1\"") {
		t.Fatal("the sink rendered a transit/1 envelope; the daemon owns that")
	}
}

// With herdr.service stopped there is no pane id at all. The send must still go
// out on the pid alone — that is the whole point of this transport.
func TestTransitSinkOmitsPaneIDWhenHerdrIsAbsent(t *testing.T) {
	t.Setenv("HERDR_PANE_ID", "")
	daemon := startFakeTransit(t, func(map[string]any) map[string]any {
		return map[string]any{"ok": true, "state": "committed"}
	})
	if err := transitTestSink(t, daemon).deliver("tick", nil); err != nil {
		t.Fatal(err)
	}
	call := daemon.calls()[0]
	if _, present := call["pane_id"]; present {
		t.Fatalf("pane_id was sent as %v; an empty hint must be omitted", call["pane_id"])
	}
	if pid, ok := call["pid"].(float64); !ok || pid <= 0 {
		t.Fatalf("pid = %v, want this process's pid", call["pid"])
	}
}

// spooled means Transit stored the message and owns the retry. everloop must
// ack, or the agent gets the same tick twice.
func TestTransitSinkAcksSpooledCustody(t *testing.T) {
	daemon := startFakeTransit(t, func(map[string]any) map[string]any {
		return map[string]any{
			"ok": true, "id": "tx_2", "state": "spooled", "local": false,
			"warning": "local fast path held: composer draft",
		}
	})
	s := transitTestSink(t, daemon)
	var logged []string
	s.log = func(message string) { logged = append(logged, message) }

	if err := s.deliver("tick", nil); err != nil {
		t.Fatalf("spooled is custody and must ack: %v", err)
	}
	if len(logged) != 1 || !strings.Contains(logged[0], "composer draft") {
		t.Fatalf("the custody warning must be surfaced, got %v", logged)
	}
}

// A structured refusal keeps its code: `agent_not_found` is a configuration
// fault, and it must not read like an unreachable daemon.
func TestTransitSinkReportsRefusalCode(t *testing.T) {
	daemon := startFakeTransit(t, func(map[string]any) map[string]any {
		return map[string]any{"ok": false, "code": "agent_not_found", "error": "target agent is not on this host"}
	})
	err := transitTestSink(t, daemon).deliver("tick", nil)
	if err == nil {
		t.Fatal("expected an error")
	}
	if !strings.Contains(err.Error(), "agent_not_found") || !strings.Contains(err.Error(), "jessica@titan") {
		t.Fatalf("error should name the code and the target, got %q", err)
	}
}

// A dead daemon is a transport failure: no code, and the socket path in the
// message, because that is the thing an operator has to go look at.
func TestTransitSinkReportsUnreachableDaemon(t *testing.T) {
	path := filepath.Join(t.TempDir(), "t.sock")
	driver, err := newTransitSocketDriver(transitSocketOptions{SocketPath: path, Log: func(string) {}})
	if err != nil {
		t.Fatal(err)
	}
	s := &transitSink{driver: driver, target: "jessica", timeout: time.Second, source: "everloop", log: func(string) {}}
	deliverErr := s.deliver("tick", nil)
	if deliverErr == nil {
		t.Fatal("expected an error")
	}
	if !strings.Contains(deliverErr.Error(), path) {
		t.Fatalf("error should name the socket, got %q", deliverErr)
	}
	if strings.Contains(deliverErr.Error(), "agent_not_found") {
		t.Fatalf("a transport failure must not look like a refusal: %q", deliverErr)
	}
}

// An `ok:true` with no state is still custody; an `ok:false` with no message
// must not produce an empty error string.
func TestTransitSinkRefusalWithoutMessage(t *testing.T) {
	daemon := startFakeTransit(t, func(map[string]any) map[string]any {
		return map[string]any{"ok": false}
	})
	err := transitTestSink(t, daemon).deliver("tick", nil)
	if err == nil || !strings.Contains(err.Error(), "refused") {
		t.Fatalf("a bare refusal must still say something, got %v", err)
	}
}

// The body has to fit Transit's 64 KiB store bound. Over it the daemon answers
// `body_too_large`, which is permanent: the drain loop would retry it every
// poll forever and block every message behind it.
func TestTransitBodyFitsTheStoreBound(t *testing.T) {
	meta := map[string]string{"kind": "tick", "loop": "chatty", "event_id": "ev_0123456789"}
	content := strings.Repeat("x", maxEventBytes+2000)
	body := transitBody("everloop", content, meta)
	if len(body) > transitMaxBodyBytes {
		t.Fatalf("body is %d bytes, want <= %d", len(body), transitMaxBodyBytes)
	}
	if !strings.HasPrefix(body, "<channel source=\"everloop\"") || !strings.HasSuffix(body, "\n</channel>") {
		t.Fatal("clipping must leave the channel envelope well-formed")
	}
	if !strings.Contains(body, "output truncated") {
		t.Fatal("a dropped tail must be visible in the body, not silent")
	}
}

func TestTransitBodyLeavesAFittingEnvelopeAlone(t *testing.T) {
	meta := map[string]string{"kind": "tick"}
	if got, want := transitBody("everloop", "tick", meta), envelope("everloop", "tick", meta); got != want {
		t.Fatalf("body = %q, want the unmodified envelope %q", got, want)
	}
}

// Pathological meta plus no room for content still has to fit rather than
// wedge the queue.
func TestTransitBodyClampsWhenContentCannotShrinkEnough(t *testing.T) {
	meta := map[string]string{"kind": "tick", "note": strings.Repeat("m", 4096)}
	body := transitBody("everloop", strings.Repeat("y", transitMaxBodyBytes*2), meta)
	if len(body) > transitMaxBodyBytes {
		t.Fatalf("body is %d bytes, want <= %d", len(body), transitMaxBodyBytes)
	}
}

func TestNewSinkTransitRequiresTarget(t *testing.T) {
	t.Setenv("CHANNEL_SINK", "transit")
	t.Setenv("TRANSIT_TARGET", "")
	t.Setenv("HERDR_TARGET", "")
	t.Setenv("TRANSIT_SOCKET", filepath.Join(t.TempDir(), "t.sock"))
	if _, err := newSink("everloop"); err == nil || !strings.Contains(err.Error(), "TRANSIT_TARGET") {
		t.Fatalf("transit sink must refuse without a target, got %v", err)
	}
}

// A config written before the default flipped — HERDR_TARGET set, CHANNEL_SINK
// unset — used to be complete. It must refuse loudly and name both remedies,
// not quietly fall back to the transport whose env var happens to be set.
func TestNewSinkDefaultRefusesAPreFlipHerdrConfig(t *testing.T) {
	t.Setenv("CHANNEL_SINK", "")
	t.Setenv("TRANSIT_TARGET", "")
	t.Setenv("HERDR_TARGET", "jessica")
	dlv, err := newSink("everloop")
	if err == nil {
		t.Fatalf("an unset CHANNEL_SINK with only HERDR_TARGET must refuse, got %T", dlv)
	}
	for _, want := range []string{"defaults to transit", "TRANSIT_TARGET", "CHANNEL_SINK=herdr"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("refusal must mention %q, got %q", want, err)
		}
	}
}

// With neither target set the refusal still has to be actionable.
func TestNewSinkDefaultRefusesWithNoTargetAtAll(t *testing.T) {
	t.Setenv("CHANNEL_SINK", "")
	t.Setenv("TRANSIT_TARGET", "")
	t.Setenv("HERDR_TARGET", "")
	_, err := newSink("everloop")
	if err == nil {
		t.Fatal("expected a refusal")
	}
	for _, want := range []string{"defaults to transit", "TRANSIT_TARGET", "CHANNEL_SINK=herdr"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("refusal must mention %q, got %q", want, err)
		}
	}
}

func TestNewSinkDefaultsToTransit(t *testing.T) {
	socket := filepath.Join(t.TempDir(), "t.sock")
	t.Setenv("TRANSIT_TARGET", "jessica@titan")
	t.Setenv("TRANSIT_SOCKET", socket)
	// HERDR_TARGET being set must not pull the default back to herdr.
	t.Setenv("HERDR_TARGET", "jessica")

	// Unset, explicit, and mixed case all select transit.
	for _, value := range []string{"", "transit", "TRANSIT", " Transit "} {
		t.Setenv("CHANNEL_SINK", value)
		dlv, err := newSink("everloop")
		if err != nil {
			t.Fatal(err)
		}
		s, ok := dlv.(*transitSink)
		if !ok {
			t.Fatalf("CHANNEL_SINK=%q selected %T, want *transitSink", value, dlv)
		}
		if s.target != "jessica@titan" || s.source != "everloop" {
			t.Fatalf("target=%q source=%q", s.target, s.source)
		}
		if s.timeout != time.Duration(transitDefaultTimeoutMS)*time.Millisecond {
			t.Fatalf("timeout = %v, want the %dms default", s.timeout, transitDefaultTimeoutMS)
		}
		if got := s.driver.(*transitSocketDriver).socketPath(); got != socket {
			t.Fatalf("socket = %q, want %q", got, socket)
		}
	}

	// herdr is still fully supported, explicitly.
	t.Setenv("CHANNEL_SINK", "herdr")
	dlv, err := newSink("everloop")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := dlv.(*herdrSink); !ok {
		t.Fatalf("CHANNEL_SINK=herdr selected %T, want *herdrSink", dlv)
	}
}

func TestNewSinkTransitRejectsBadTimeout(t *testing.T) {
	t.Setenv("CHANNEL_SINK", "transit")
	t.Setenv("TRANSIT_TARGET", "jessica@titan")
	t.Setenv("TRANSIT_SOCKET", filepath.Join(t.TempDir(), "t.sock"))
	for _, bad := range []string{"0", "-1", "abc", "12.5"} {
		t.Setenv("TRANSIT_SEND_TIMEOUT_MS", bad)
		if _, err := newSink("everloop"); err == nil {
			t.Fatalf("TRANSIT_SEND_TIMEOUT_MS=%q must be refused", bad)
		}
	}
	t.Setenv("TRANSIT_SEND_TIMEOUT_MS", "20000")
	dlv, err := newSink("everloop")
	if err != nil {
		t.Fatal(err)
	}
	if got := dlv.(*transitSink).timeout; got != 20*time.Second {
		t.Fatalf("timeout = %v, want 20s", got)
	}
}

// The socket is resolved the way transit itself resolves it, so everloop and
// the daemon cannot disagree about where it is.
func TestResolveTransitSocketPathPrecedence(t *testing.T) {
	t.Setenv("TRANSIT_SOCKET", "/tmp/explicit.sock")
	t.Setenv("TRANSIT_DATA_DIR", "/tmp/data")
	if got, _ := resolveTransitSocketPath(transitSocketOptions{SocketPath: "/tmp/opt.sock"}); got != "/tmp/opt.sock" {
		t.Fatalf("option must win, got %q", got)
	}
	if got, _ := resolveTransitSocketPath(transitSocketOptions{}); got != "/tmp/explicit.sock" {
		t.Fatalf("TRANSIT_SOCKET must win over TRANSIT_DATA_DIR, got %q", got)
	}
	t.Setenv("TRANSIT_SOCKET", "")
	if got, _ := resolveTransitSocketPath(transitSocketOptions{}); got != "/tmp/data/transit.sock" {
		t.Fatalf("TRANSIT_DATA_DIR must be honoured, got %q", got)
	}
	t.Setenv("TRANSIT_DATA_DIR", "")
	got, err := resolveTransitSocketPath(transitSocketOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(got, filepath.Join(".local", "share", "transit", "transit.sock")) {
		t.Fatalf("default = %q, want transit's own default location", got)
	}
}

// A driver is constructed at serve startup, when the Transit daemon may not
// have come up yet. That must not stop the MCP tools from coming up.
func TestTransitDriverDoesNotDialAtConstruction(t *testing.T) {
	driver, err := newTransitSocketDriver(transitSocketOptions{
		SocketPath: filepath.Join(t.TempDir(), "absent.sock"),
		Log:        func(string) {},
	})
	if err != nil {
		t.Fatalf("construction must not require a live daemon: %v", err)
	}
	if driver.socketPath() == "" {
		t.Fatal("driver has no socket path")
	}
}

// A daemon that accepts and says nothing must fail the delivery within the
// bound rather than holding the single-threaded drain loop open.
func TestTransitSinkTimesOutASilentDaemon(t *testing.T) {
	path := filepath.Join(t.TempDir(), "t.sock")
	listener, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	accepted := make(chan net.Conn, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		accepted <- conn
	}()

	driver, err := newTransitSocketDriver(transitSocketOptions{SocketPath: path, Log: func(string) {}})
	if err != nil {
		t.Fatal(err)
	}
	s := &transitSink{driver: driver, target: "jessica", timeout: 250 * time.Millisecond, source: "everloop", log: func(string) {}}
	start := time.Now()
	deliverErr := s.deliver("tick", nil)
	if deliverErr == nil {
		t.Fatal("expected a timeout error")
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("delivery took %v, want the 250ms bound to apply", elapsed)
	}
	select {
	case conn := <-accepted:
		conn.Close()
	default:
	}
}
