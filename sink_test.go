package main

// Sink tests: envelope rendering, opencode session resolve + prompt_async
// injection, hermes V2 HMAC signing + idempotency header, and the herdr sink's
// argv construction against a fake `herdr` binary.

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

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

func TestOpencodeSinkResolvesSessionAndInjects(t *testing.T) {
	var injected []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == "GET" && r.URL.Path == "/session":
			json.NewEncoder(w).Encode([]ocSession{{ID: "ses_other", Title: "other"}, {ID: "ses_hit", Title: "bot", Directory: "/proj"}})
		case r.Method == "POST" && r.URL.Path == "/session/ses_hit/prompt_async":
			var body struct {
				Parts []struct {
					Text string `json:"text"`
				} `json:"parts"`
			}
			json.NewDecoder(r.Body).Decode(&body)
			injected = append(injected, body.Parts[0].Text)
			w.WriteHeader(http.StatusNoContent)
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	s := &opencodeSink{base: srv.URL, title: "bot", directory: "/proj", source: "tincan", client: srv.Client()}
	if err := s.deliver("ping", map[string]string{"from": "clem", "event_id": "abc"}); err != nil {
		t.Fatal(err)
	}
	if len(injected) != 1 {
		t.Fatalf("expected 1 injection, got %d", len(injected))
	}
	if !strings.Contains(injected[0], `<channel source="tincan" `) || !strings.Contains(injected[0], "ping") {
		t.Fatalf("bad envelope: %q", injected[0])
	}
	if s.resolved != "ses_hit" {
		t.Fatalf("resolved = %q, want ses_hit", s.resolved)
	}
}

func TestOpencodeSinkCreatesSessionWhenMissing(t *testing.T) {
	created := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == "GET" && r.URL.Path == "/session":
			json.NewEncoder(w).Encode([]ocSession{})
		case r.Method == "POST" && r.URL.Path == "/session":
			created = true
			json.NewEncoder(w).Encode(ocSession{ID: "ses_new", Title: "bot"})
		case strings.HasSuffix(r.URL.Path, "/prompt_async"):
			w.WriteHeader(http.StatusNoContent)
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	s := &opencodeSink{base: srv.URL, title: "bot", source: "tincan", client: srv.Client()}
	if err := s.deliver("hi", map[string]string{}); err != nil {
		t.Fatal(err)
	}
	if !created || s.resolved != "ses_new" {
		t.Fatalf("created=%v resolved=%q", created, s.resolved)
	}
}

func TestOpencodeSinkReresolvesOn404(t *testing.T) {
	var posts []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == "GET" && r.URL.Path == "/session":
			json.NewEncoder(w).Encode([]ocSession{{ID: "ses_live", Title: "bot"}})
		case strings.HasSuffix(r.URL.Path, "/prompt_async"):
			posts = append(posts, r.URL.Path)
			if strings.Contains(r.URL.Path, "ses_stale") {
				http.NotFound(w, r)
				return
			}
			w.WriteHeader(http.StatusNoContent)
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	s := &opencodeSink{base: srv.URL, title: "bot", source: "tincan", client: srv.Client(), resolved: "ses_stale"}
	if err := s.deliver("hi", map[string]string{}); err != nil {
		t.Fatal(err)
	}
	if len(posts) != 2 || !strings.Contains(posts[1], "ses_live") {
		t.Fatalf("expected retry against ses_live, posts=%v", posts)
	}
}

func TestHermesSinkSignsV2AndSendsRequestID(t *testing.T) {
	secret := "topsecret"
	var gotSig, gotTS, gotReqID string
	var gotBody []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotSig = r.Header.Get("X-Webhook-Signature-V2")
		gotTS = r.Header.Get("X-Webhook-Timestamp")
		gotReqID = r.Header.Get("X-Request-ID")
		gotBody, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	s := &hermesSink{url: srv.URL, secret: secret, source: "tincan", client: srv.Client()}
	meta := map[string]string{"event_id": "xyz", "from": "clem"}
	if err := s.deliver("ping", meta); err != nil {
		t.Fatal(err)
	}
	if gotTS == "" || gotSig == "" {
		t.Fatalf("missing signature headers: ts=%q sig=%q", gotTS, gotSig)
	}
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(gotTS + "." + string(gotBody)))
	if gotSig != hex.EncodeToString(mac.Sum(nil)) {
		t.Fatal("bad V2 signature")
	}
	if gotReqID != "tincan-xyz" {
		t.Fatalf("X-Request-ID = %q", gotReqID)
	}
	var payload struct {
		Body string            `json:"body"`
		Meta map[string]string `json:"meta"`
	}
	if err := json.Unmarshal(gotBody, &payload); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(payload.Body, `<channel source="tincan" `) || !strings.Contains(payload.Body, "ping") {
		t.Fatalf("bad body: %q", payload.Body)
	}
}

func TestHermesSinkRequiresURL(t *testing.T) {
	t.Setenv("CHANNEL_SINK", "hermes")
	t.Setenv("HERMES_WEBHOOK_URL", "")
	if _, err := newSink("tincan", nil); err == nil {
		t.Fatal("expected error without HERMES_WEBHOOK_URL")
	}
}

// --- herdr -------------------------------------------------------------------

// argSep separates the recorded arguments in the fake binary's log. A plain
// newline would not do: the envelope contains newlines, and the whole point of
// these tests is proving it arrives as ONE argv element rather than several.
const argSep = "<<<ARGSEP>>>"

// fakeHerdr writes a stand-in `herdr` into a temp dir and points the sink at
// it. It records its own argv and the two env vars the real CLI resolves its
// server with, then exits $FAKE_HERDR_EXIT after writing $FAKE_HERDR_STDERR.
// Returned: the script path, and readers for the argv and env it saw.
func fakeHerdr(t *testing.T) (bin string, argv func() []string, childEnv func() string) {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("fake herdr is a /bin/sh script")
	}
	dir := t.TempDir()
	bin = filepath.Join(dir, "herdr")
	argvLog := filepath.Join(dir, "argv")
	envLog := filepath.Join(dir, "env")
	script := `#!/bin/sh
: > "$FAKE_HERDR_ARGV"
for a in "$@"; do printf '%s` + argSep + `' "$a" >> "$FAKE_HERDR_ARGV"; done
printf 'HERDR_SOCKET_PATH=%s\nHERDR_SESSION=%s\n' "$HERDR_SOCKET_PATH" "$HERDR_SESSION" > "$FAKE_HERDR_ENV"
[ -n "$FAKE_HERDR_STDERR" ] && printf '%s' "$FAKE_HERDR_STDERR" >&2
exit "${FAKE_HERDR_EXIT:-0}"
`
	if err := os.WriteFile(bin, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("FAKE_HERDR_ARGV", argvLog)
	t.Setenv("FAKE_HERDR_ENV", envLog)

	argv = func() []string {
		t.Helper()
		b, err := os.ReadFile(argvLog)
		if err != nil {
			t.Fatalf("fake herdr never ran: %v", err)
		}
		parts := strings.Split(string(b), argSep)
		return parts[:len(parts)-1] // trailing separator
	}
	childEnv = func() string {
		t.Helper()
		b, err := os.ReadFile(envLog)
		if err != nil {
			t.Fatalf("fake herdr never ran: %v", err)
		}
		return string(b)
	}
	return bin, argv, childEnv
}

func TestHerdrSinkExecsAgentPromptWithEnvelopeAsOneArg(t *testing.T) {
	bin, argv, _ := fakeHerdr(t)

	s := &herdrSink{bin: bin, target: "clem", timeoutMS: 120000, source: "everloop"}
	if err := s.deliver("reconcile the ledger", map[string]string{"kind": "tick", "loop": "recon", "event_id": "abc"}); err != nil {
		t.Fatal(err)
	}

	got := argv()
	want := []string{
		"agent", "prompt", "clem",
		envelope("everloop", "reconcile the ledger", map[string]string{"kind": "tick", "loop": "recon", "event_id": "abc"}),
		"--wait", "--timeout", "120000",
	}
	if len(got) != len(want) {
		t.Fatalf("argv length = %d, want %d: %q", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("argv[%d] = %q, want %q", i, got[i], want[i])
		}
	}
	// The envelope is multi-line; landing as one element is the whole claim.
	if !strings.Contains(got[3], "\n") || !strings.HasPrefix(got[3], `<channel source="everloop" `) {
		t.Fatalf("envelope arg = %q", got[3])
	}
}

func TestHerdrSinkNonZeroExitReturnsErrorWithStderrCode(t *testing.T) {
	bin, _, _ := fakeHerdr(t)
	t.Setenv("FAKE_HERDR_EXIT", "1")
	t.Setenv("FAKE_HERDR_STDERR", `{"error":{"code":"agent_not_found","message":"agent target clem not found"},"id":"cli:agent:prompt"}`)

	s := &herdrSink{bin: bin, target: "clem", timeoutMS: 5000, source: "everloop"}
	err := s.deliver("ping", map[string]string{"kind": "tick"})
	if err == nil {
		// A nil here would ack the message; the spool would drop a tick that
		// never reached an agent.
		t.Fatal("expected an error so the message stays claimed for the next poll")
	}
	if !strings.Contains(err.Error(), "agent_not_found") {
		t.Fatalf("error should carry herdr's stderr JSON, got: %v", err)
	}
}

func TestHerdrSinkMissingBinaryIsAnError(t *testing.T) {
	s := &herdrSink{bin: filepath.Join(t.TempDir(), "definitely-not-here"), target: "clem", timeoutMS: 1000, source: "everloop"}
	if err := s.deliver("ping", nil); err == nil {
		t.Fatal("expected an error when the herdr binary does not exist")
	}
}

func TestHerdrSinkPassesHerdrEnvThrough(t *testing.T) {
	bin, _, childEnv := fakeHerdr(t)
	t.Setenv("HERDR_SOCKET_PATH", "/run/user/1000/herdr/test.sock")
	t.Setenv("HERDR_SESSION", "titan")

	s := &herdrSink{bin: bin, target: "clem", timeoutMS: 1000, source: "everloop"}
	if err := s.deliver("ping", nil); err != nil {
		t.Fatal(err)
	}
	got := childEnv()
	if !strings.Contains(got, "HERDR_SOCKET_PATH=/run/user/1000/herdr/test.sock") ||
		!strings.Contains(got, "HERDR_SESSION=titan") {
		t.Fatalf("herdr env did not reach the CLI untouched: %q", got)
	}
}

func TestHerdrSinkDoesNotGoThroughAShell(t *testing.T) {
	bin, argv, _ := fakeHerdr(t)
	sentinel := filepath.Join(t.TempDir(), "pwned")
	hostile := "look; touch " + sentinel + " && echo $(whoami) `id` \"quoted\""

	s := &herdrSink{bin: bin, target: "clem", timeoutMS: 1000, source: "everloop"}
	if err := s.deliver(hostile, map[string]string{"kind": "message"}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(sentinel); err == nil {
		t.Fatal("content was interpreted by a shell")
	}
	if got := argv()[3]; !strings.Contains(got, hostile) {
		t.Fatalf("content mangled in transit: %q", got)
	}
}

func TestNewSinkHerdrRequiresTarget(t *testing.T) {
	t.Setenv("CHANNEL_SINK", "herdr")
	t.Setenv("HERDR_TARGET", "")
	if _, err := newSink("everloop", nil); err == nil {
		t.Fatal("expected an error without HERDR_TARGET")
	}
}

func TestNewSinkHerdrDefaults(t *testing.T) {
	t.Setenv("CHANNEL_SINK", "herdr")
	t.Setenv("HERDR_TARGET", "clem")
	t.Setenv("HERDR_BIN", "")
	t.Setenv("HERDR_PROMPT_TIMEOUT_MS", "")

	dlv, err := newSink("everloop", nil)
	if err != nil {
		t.Fatal(err)
	}
	s, ok := dlv.(*herdrSink)
	if !ok {
		t.Fatalf("sink = %T, want *herdrSink", dlv)
	}
	if s.bin != "herdr" || s.target != "clem" || s.timeoutMS != 120000 || s.source != "everloop" {
		t.Fatalf("defaults wrong: %+v", *s)
	}
}

func TestNewSinkHerdrHonorsBinAndTimeout(t *testing.T) {
	t.Setenv("CHANNEL_SINK", "herdr")
	t.Setenv("HERDR_TARGET", "ws/agent")
	t.Setenv("HERDR_BIN", "/opt/herdr/bin/herdr")
	t.Setenv("HERDR_PROMPT_TIMEOUT_MS", "45000")

	dlv, err := newSink("everloop", nil)
	if err != nil {
		t.Fatal(err)
	}
	s := dlv.(*herdrSink)
	if s.bin != "/opt/herdr/bin/herdr" || s.timeoutMS != 45000 {
		t.Fatalf("bin=%q timeout=%d", s.bin, s.timeoutMS)
	}
}

func TestNewSinkHerdrRejectsBadTimeout(t *testing.T) {
	t.Setenv("CHANNEL_SINK", "herdr")
	t.Setenv("HERDR_TARGET", "clem")
	for _, v := range []string{"soon", "0", "-1", "12.5"} {
		t.Setenv("HERDR_PROMPT_TIMEOUT_MS", v)
		if _, err := newSink("everloop", nil); err == nil {
			// Falling back to the default would look like a slow agent, with
			// nothing saying the setting never took.
			t.Fatalf("HERDR_PROMPT_TIMEOUT_MS=%q was accepted", v)
		}
	}
}

// The fake binary resolved off PATH, i.e. exactly what the "herdr" default
// means at delivery time rather than at construction time.
func TestHerdrSinkResolvesBinFromPATH(t *testing.T) {
	bin, argv, _ := fakeHerdr(t)
	t.Setenv("PATH", filepath.Dir(bin)+string(os.PathListSeparator)+os.Getenv("PATH"))

	s := &herdrSink{bin: "herdr", target: "clem", timeoutMS: 1000, source: "everloop"}
	if err := s.deliver("ping", nil); err != nil {
		t.Fatal(err)
	}
	if got := argv(); len(got) != 7 || got[0] != "agent" {
		t.Fatalf("argv = %q", got)
	}
}

func TestDefaultSinkIsClaude(t *testing.T) {
	t.Setenv("CHANNEL_SINK", "")
	if _, err := newSink("tincan", &stdoutWriter{}); err != nil {
		t.Fatal(err)
	}
}

func TestNoneSinkIsToolsOnly(t *testing.T) {
	for _, v := range []string{"none", "tools"} {
		t.Setenv("CHANNEL_SINK", v)
		dlv, err := newSink("tincan", &stdoutWriter{})
		if err != nil {
			t.Fatal(err)
		}
		if dlv != nil {
			t.Fatalf("CHANNEL_SINK=%s: sink = %T, want nil (no drain loop)", v, dlv)
		}
	}
}
