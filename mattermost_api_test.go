package main

// Mattermost sink tests, against a fake Mattermost SERVER rather than a fake
// driver: what decides whether a firing wakes an agent is which account the
// post is written by, which conversation it lands in, and what the sink does
// with a refusal — none of which a stubbed driver can show.
//
// The refusals matter as much as the delivery here. This transport is the only
// one everloop has whose worst misconfiguration is *silent*: a post addressed
// to the sending identity is accepted by the server and then dropped by the
// recipient's own listener, so nobody is woken and nothing is logged. Both
// halves of that guard — the offline one at startup and the resolved one on
// first delivery — are asserted.

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// Mattermost ids are 26 characters; these are the shape a real one has, so the
// offline target check sees exactly what production gives it.
const (
	testSenderID    = "everloopsenderaaaaaaaaaaaa"
	testRecipientID = "normbbbbbbbbbbbbbbbbbbbbbb"
	testChannelID   = "dmchannelcccccccccccccccc0"
	testTokenEnv    = "MATTERMOST_TEST_TOKEN"
)

type mattermostCall struct {
	Method string
	Path   string
	Auth   string
	Body   any
}

type fakeMattermost struct {
	server *httptest.Server
	mu     sync.Mutex
	calls  []mattermostCall
}

// startFakeMattermost serves the four endpoints this sink uses. `refuse`, when
// non-nil, gets first look at every request and can answer it instead.
func startFakeMattermost(t *testing.T, refuse func(call mattermostCall) (int, string, string)) *fakeMattermost {
	t.Helper()
	fake := &fakeMattermost{}
	fake.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		call := mattermostCall{
			Method: r.Method,
			Path:   strings.TrimPrefix(r.URL.Path, "/api/v4"),
			Auth:   r.Header.Get("Authorization"),
		}
		if len(raw) > 0 {
			_ = json.Unmarshal(raw, &call.Body)
		}
		fake.mu.Lock()
		fake.calls = append(fake.calls, call)
		fake.mu.Unlock()

		if refuse != nil {
			if status, id, message := refuse(call); status != 0 {
				w.WriteHeader(status)
				_ = json.NewEncoder(w).Encode(map[string]string{"id": id, "message": message})
				return
			}
		}

		switch {
		case call.Path == "/users/me":
			answerJSON(w, map[string]string{"id": testSenderID})
		case strings.HasPrefix(call.Path, "/users/username/"):
			name := strings.TrimPrefix(call.Path, "/users/username/")
			if name != "norm" {
				w.WriteHeader(http.StatusNotFound)
				_ = json.NewEncoder(w).Encode(map[string]string{
					"id":      "store.sql_user.missing_account.const",
					"message": "Unable to find the user.",
				})
				return
			}
			answerJSON(w, map[string]string{"id": testRecipientID})
		case call.Path == "/channels/direct":
			answerJSON(w, map[string]string{"id": testChannelID})
		case call.Path == "/posts":
			body, _ := call.Body.(map[string]any)
			channel, _ := body["channel_id"].(string)
			answerJSON(w, map[string]string{"id": "postddddddddddddddddddddd0", "channel_id": channel})
		default:
			t.Errorf("fake mattermost got an unexpected request: %s %s", call.Method, call.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(fake.server.Close)
	return fake
}

func answerJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

func (f *fakeMattermost) seen() []mattermostCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]mattermostCall(nil), f.calls...)
}

// posts returns the message of every post that reached the server, keyed by the
// channel it landed in — the two things a recipient's listener acts on.
func (f *fakeMattermost) posts() []mattermostCall {
	var out []mattermostCall
	for _, call := range f.seen() {
		if call.Path == "/posts" {
			out = append(out, call)
		}
	}
	return out
}

// writeProfile writes a mattermost-agents profile for the SENDING identity,
// shaped like the real ones on the fleet (extra listener fields included, since
// everloop must read a file that project owns without choking on them).
func writeProfile(t *testing.T, url string, pinSelf bool, extra ...mattermostConnection) string {
	t.Helper()
	primary := map[string]any{
		"id":               "ocai",
		"url":              url,
		"tokenEnv":         testTokenEnv,
		"tokenSecret":      "MATTERMOST_AGENT_EVERLOOP_TOKEN",
		"channelIds":       []string{},
		"watchMemberships": true,
		"pollIntervalMs":   5000,
	}
	if pinSelf {
		primary["expectedUserId"] = testSenderID
	}
	connections := []any{primary}
	for _, conn := range extra {
		connections = append(connections, conn)
	}
	raw, err := json.Marshal(map[string]any{"version": 1, "connections": connections})
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "everloop.json")
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// mattermostTestSink builds the sink the way an operator does — through the
// environment — so every test also covers the configuration contract.
func mattermostTestSink(t *testing.T, url, target string, pinSelf bool) sink {
	t.Helper()
	t.Setenv("CHANNEL_SINK", "mattermost")
	t.Setenv("MATTERMOST_TARGET", target)
	t.Setenv("MATTERMOST_PROFILE", writeProfile(t, url, pinSelf))
	t.Setenv(testTokenEnv, "sender-token")
	dlv, err := newSink("everloop")
	if err != nil {
		t.Fatal(err)
	}
	return dlv
}

func TestMattermostSinkDeliversTheFiringAsADMFromTheSendingAccount(t *testing.T) {
	fake := startFakeMattermost(t, nil)
	s := mattermostTestSink(t, fake.server.URL, testRecipientID, true)

	if err := s.deliver("reconcile the ledger", map[string]string{"kind": "tick", "loop": "recon"}); err != nil {
		t.Fatal(err)
	}

	var pair []any
	for _, call := range fake.seen() {
		if call.Path == "/channels/direct" {
			pair, _ = call.Body.([]any)
		}
		if call.Auth != "Bearer sender-token" {
			t.Fatalf("%s %s went out as %q, want the sending profile's credential", call.Method, call.Path, call.Auth)
		}
	}
	if len(pair) != 2 || pair[0] != testSenderID || pair[1] != testRecipientID {
		t.Fatalf("direct channel opened for %v, want [%s %s]", pair, testSenderID, testRecipientID)
	}

	posts := fake.posts()
	if len(posts) != 1 {
		t.Fatalf("want exactly one post, got %d", len(posts))
	}
	body, _ := posts[0].Body.(map[string]any)
	if body["channel_id"] != testChannelID {
		t.Fatalf("post landed in %v, want the DM channel %s", body["channel_id"], testChannelID)
	}
	message, _ := body["message"].(string)
	if !strings.HasPrefix(message, `<channel source="everloop" kind="tick" loop="recon">`) ||
		!strings.Contains(message, "reconcile the ledger") {
		t.Fatalf("envelope not delivered verbatim: %q", message)
	}
}

func TestMattermostSinkResolvesAUsernameTarget(t *testing.T) {
	fake := startFakeMattermost(t, nil)
	s := mattermostTestSink(t, fake.server.URL, "@norm", true)

	if err := s.deliver("tick", nil); err != nil {
		t.Fatal(err)
	}

	for _, call := range fake.seen() {
		if call.Path == "/channels/direct" {
			pair, _ := call.Body.([]any)
			if len(pair) != 2 || pair[1] != testRecipientID {
				t.Fatalf("@norm resolved to %v, want %s", pair, testRecipientID)
			}
			return
		}
	}
	t.Fatalf("no direct channel was opened: %+v", fake.seen())
}

// A recipient who does not exist is a configuration fault, and must be legible
// as one: Mattermost's own error id travels into the error an operator reads.
func TestMattermostSinkReportsARefusalWithItsCode(t *testing.T) {
	fake := startFakeMattermost(t, nil)
	s := mattermostTestSink(t, fake.server.URL, "@nobody", true)

	err := s.deliver("tick", nil)
	if err == nil {
		t.Fatal("a firing that cannot be delivered must be reported, not swallowed")
	}
	if !strings.Contains(err.Error(), "store.sql_user.missing_account.const") {
		t.Fatalf("error must name the code, got %q", err)
	}
	if len(fake.posts()) != 0 {
		t.Fatalf("nothing may be posted when the recipient does not resolve: %+v", fake.posts())
	}
}

// A server that cannot be reached carries no code — the difference between
// "your config is wrong" and "the delivery outcome is unknown".
func TestMattermostSinkTransportFailureHasNoCode(t *testing.T) {
	fake := startFakeMattermost(t, nil)
	s := mattermostTestSink(t, fake.server.URL, testRecipientID, true)
	fake.server.Close()

	err := s.deliver("tick", nil)
	if err == nil {
		t.Fatal("expected an error")
	}
	for _, code := range []string{"store.sql_user", "app_error", "everloop_self_delivery"} {
		if strings.Contains(err.Error(), code) {
			t.Fatalf("an unreachable server must not be labelled %s: %q", code, err)
		}
	}
}

// The spool retries a failed delivery on the next poll, so the sink must be
// willing to deliver the same firing again: the recipient gets it once the
// server recovers, rather than the firing being lost with the first attempt.
func TestMattermostSinkRedeliversAfterAFailure(t *testing.T) {
	var refused bool
	fake := startFakeMattermost(t, func(call mattermostCall) (int, string, string) {
		if call.Path == "/posts" && !refused {
			refused = true
			return http.StatusBadGateway, "", "upstream is down"
		}
		return 0, "", ""
	})
	s := mattermostTestSink(t, fake.server.URL, testRecipientID, true)

	if err := s.deliver("reconcile the ledger", map[string]string{"kind": "tick"}); err == nil {
		t.Fatal("the first attempt must be reported as failed so the spool keeps the message")
	}
	if err := s.deliver("reconcile the ledger", map[string]string{"kind": "tick"}); err != nil {
		t.Fatalf("the retry must reach the recipient: %v", err)
	}

	posts := fake.posts()
	if len(posts) != 2 {
		t.Fatalf("want the failed attempt and the retry, got %d posts", len(posts))
	}
	body, _ := posts[1].Body.(map[string]any)
	if message, _ := body["message"].(string); !strings.Contains(message, "reconcile the ledger") {
		t.Fatalf("the retry delivered %q", message)
	}
	if body["channel_id"] != testChannelID {
		t.Fatalf("the retry landed in %v, want the DM channel", body["channel_id"])
	}
}

// The silent-drop guard, offline half: a firing addressed to the sending
// identity would be posted and then ignored by that account's own listener, so
// serve must refuse to start rather than deliver into a void.
func TestNewSinkMattermostRefusesDeliveryToTheSendingIdentity(t *testing.T) {
	fake := startFakeMattermost(t, nil)
	t.Setenv("CHANNEL_SINK", "mattermost")
	t.Setenv("MATTERMOST_TARGET", testSenderID)
	t.Setenv("MATTERMOST_PROFILE", writeProfile(t, fake.server.URL, true))
	t.Setenv(testTokenEnv, "sender-token")

	_, err := newSink("everloop")
	if err == nil {
		t.Fatal("a sink addressed to its own sending identity must refuse at startup")
	}
	if !strings.Contains(err.Error(), "own identity") {
		t.Fatalf("the refusal must say why, got %q", err)
	}
	if len(fake.seen()) != 0 {
		t.Fatalf("startup must not contact the server: %+v", fake.seen())
	}
}

// The same guard for the form that can only be resolved against the server: a
// username, or an unpinned profile, is checked on first delivery instead — and
// still refuses rather than posting where nobody will read it.
func TestMattermostSinkRefusesSelfDeliveryResolvedAtRuntime(t *testing.T) {
	fake := startFakeMattermost(t, nil)
	s := mattermostTestSink(t, fake.server.URL, testSenderID, false)

	err := s.deliver("tick", nil)
	if err == nil {
		t.Fatal("posting to our own account drops the firing in silence; it must be reported")
	}
	if !strings.Contains(err.Error(), "everloop_self_delivery") {
		t.Fatalf("error should carry the self-delivery code, got %q", err)
	}
	if len(fake.posts()) != 0 {
		t.Fatalf("nothing may be posted: %+v", fake.posts())
	}
}

func TestNewSinkMattermostIsDefaultAndRefusesWithoutARecipient(t *testing.T) {
	t.Setenv("CHANNEL_SINK", "")
	t.Setenv("MATTERMOST_TARGET", "")
	t.Setenv("HERDR_TARGET", "")
	_, err := newSink("everloop")
	if err == nil {
		t.Fatal("an unconfigured default sink must refuse, not run without delivery")
	}
	if !strings.Contains(err.Error(), "MATTERMOST_TARGET") {
		t.Fatalf("refusal must name what is missing, got %q", err)
	}

	// A config from before the default changed: complete for herdr, incomplete
	// now. It gets both remedies rather than a guess.
	t.Setenv("HERDR_TARGET", "norm")
	_, err = newSink("everloop")
	if err == nil {
		t.Fatal("a pre-change herdr config must not be silently adopted")
	}
	for _, remedy := range []string{"MATTERMOST_TARGET", "CHANNEL_SINK=herdr"} {
		if !strings.Contains(err.Error(), remedy) {
			t.Fatalf("refusal must name %s, got %q", remedy, err)
		}
	}
}

func TestNewSinkMattermostRefusesWithoutASendingProfile(t *testing.T) {
	t.Setenv("CHANNEL_SINK", "mattermost")
	t.Setenv("MATTERMOST_TARGET", testRecipientID)
	t.Setenv("MATTERMOST_PROFILE", "")
	_, err := newSink("everloop")
	if err == nil {
		t.Fatal("without a sending identity there is nothing to post as; that must refuse")
	}
	if !strings.Contains(err.Error(), "MATTERMOST_PROFILE") {
		t.Fatalf("refusal must name the missing profile, got %q", err)
	}
}

func TestNewSinkMattermostRefusesAnUnusableRecipient(t *testing.T) {
	fake := startFakeMattermost(t, nil)
	profile := writeProfile(t, fake.server.URL, true)
	t.Setenv("CHANNEL_SINK", "mattermost")
	t.Setenv("MATTERMOST_PROFILE", profile)
	t.Setenv(testTokenEnv, "sender-token")
	// A bare name (no @) is refused rather than guessed at: it is neither a
	// username nor an id, and guessing wrong posts a firing to a stranger.
	for _, bad := range []string{"norm", "@", "not-an-id", testRecipientID + "extra", "NORMBBBBBBBBBBBBBBBBBBBBBB"} {
		t.Setenv("MATTERMOST_TARGET", bad)
		if _, err := newSink("everloop"); err == nil {
			t.Fatalf("MATTERMOST_TARGET=%q must be refused", bad)
		}
	}
	t.Setenv("MATTERMOST_TARGET", "@norm")
	if _, err := newSink("everloop"); err != nil {
		t.Fatalf("@norm must be accepted: %v", err)
	}
}

func TestNewSinkMattermostRejectsBadTimeout(t *testing.T) {
	fake := startFakeMattermost(t, nil)
	t.Setenv("CHANNEL_SINK", "mattermost")
	t.Setenv("MATTERMOST_TARGET", testRecipientID)
	t.Setenv("MATTERMOST_PROFILE", writeProfile(t, fake.server.URL, true))
	t.Setenv(testTokenEnv, "sender-token")
	for _, bad := range []string{"0", "-1", "abc", "12.5"} {
		t.Setenv("MATTERMOST_POST_TIMEOUT_MS", bad)
		if _, err := newSink("everloop"); err == nil {
			t.Fatalf("MATTERMOST_POST_TIMEOUT_MS=%q must be refused", bad)
		}
	}
	t.Setenv("MATTERMOST_POST_TIMEOUT_MS", "9000")
	dlv, err := newSink("everloop")
	if err != nil {
		t.Fatal(err)
	}
	if got := dlv.(*mattermostSink).timeout; got != 9*time.Second {
		t.Fatalf("timeout = %v, want 9s", got)
	}
}

// Two identities in one profile and no MATTERMOST_CONNECTION: picking by
// position would post half the fleet's firings from the wrong account.
func TestNewSinkMattermostRefusesAnAmbiguousProfile(t *testing.T) {
	fake := startFakeMattermost(t, nil)
	profile := writeProfile(t, fake.server.URL, true, mattermostConnection{
		ID:       "other",
		URL:      fake.server.URL,
		TokenEnv: testTokenEnv,
	})
	t.Setenv("CHANNEL_SINK", "mattermost")
	t.Setenv("MATTERMOST_TARGET", testRecipientID)
	t.Setenv("MATTERMOST_PROFILE", profile)
	t.Setenv(testTokenEnv, "sender-token")

	_, err := newSink("everloop")
	if err == nil || !strings.Contains(err.Error(), "MATTERMOST_CONNECTION") {
		t.Fatalf("an ambiguous profile must be refused with the remedy named, got %v", err)
	}

	t.Setenv("MATTERMOST_CONNECTION", "other")
	if _, err := newSink("everloop"); err != nil {
		t.Fatalf("naming the connection must resolve it: %v", err)
	}
}

// A credential that does not resolve must not stop `serve` from coming up: the
// MCP tools are how an operator fixes the config, and a box whose secret store
// is asleep at boot would otherwise lose them too. The failure surfaces on
// delivery instead, where the spool retries it.
func TestMattermostSinkStartsWithoutResolvingTheCredential(t *testing.T) {
	fake := startFakeMattermost(t, nil)
	t.Setenv("CHANNEL_SINK", "mattermost")
	t.Setenv("MATTERMOST_TARGET", testRecipientID)
	t.Setenv("MATTERMOST_PROFILE", writeProfile(t, fake.server.URL, true))
	t.Setenv(testTokenEnv, "")
	t.Setenv("PATH", "") // no `secret` CLI to fall back to

	dlv, err := newSink("everloop")
	if err != nil {
		t.Fatalf("serve must still come up: %v", err)
	}
	err = dlv.deliver("tick", nil)
	if err == nil || !strings.Contains(err.Error(), "credential") {
		t.Fatalf("delivery must report the missing credential, got %v", err)
	}
	if len(fake.posts()) != 0 {
		t.Fatalf("nothing may be posted without a credential: %+v", fake.posts())
	}
}

func TestMattermostBodyFitsThePostBound(t *testing.T) {
	body := mattermostBody("everloop", strings.Repeat("x", 5*mattermostMaxPostBytes), map[string]string{"kind": "tick", "loop": "chatty"})
	if len(body) > mattermostMaxPostBytes {
		t.Fatalf("body is %d bytes, over the server's bound of %d", len(body), mattermostMaxPostBytes)
	}
	if !strings.HasPrefix(body, `<channel source="everloop" kind="tick" loop="chatty">`) || !strings.HasSuffix(body, "\n</channel>") {
		t.Fatalf("the clip must land inside the envelope, not on it: %q", body[:80]+"..."+body[len(body)-40:])
	}
}

func TestMattermostBodyLeavesAFittingEnvelopeAlone(t *testing.T) {
	want := envelope("everloop", "short", map[string]string{"kind": "tick"})
	if got := mattermostBody("everloop", "short", map[string]string{"kind": "tick"}); got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

// The whole delivery is bounded, not each request: a server that accepts the
// connection and then says nothing must not hold the drain loop open.
func TestMattermostSinkTimesOutASilentServer(t *testing.T) {
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		<-release
	}))
	// Cleanups run last-in-first-out, so the parked handler is released before
	// Close waits for it.
	t.Cleanup(server.Close)
	t.Cleanup(func() { close(release) })
	t.Setenv("CHANNEL_SINK", "mattermost")
	t.Setenv("MATTERMOST_TARGET", testRecipientID)
	t.Setenv("MATTERMOST_PROFILE", writeProfile(t, server.URL, true))
	t.Setenv(testTokenEnv, "sender-token")
	t.Setenv("MATTERMOST_POST_TIMEOUT_MS", "150")
	dlv, err := newSink("everloop")
	if err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go func() { done <- dlv.deliver("tick", nil) }()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("a silent server must not look like a delivery")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("deliver did not honour MATTERMOST_POST_TIMEOUT_MS")
	}
}

func TestMattermostProfileMustNameACredential(t *testing.T) {
	raw, err := json.Marshal(map[string]any{"version": 1, "connections": []any{
		map[string]any{"id": "ocai", "url": "https://mattermost.example"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "no-token.json")
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadMattermostConnection(path, ""); err == nil {
		t.Fatal("a profile with no tokenEnv and no tokenSecret cannot send; that must refuse")
	}
	if _, err := loadMattermostConnection(filepath.Join(t.TempDir(), "absent.json"), ""); err == nil {
		t.Fatal("a missing profile must refuse")
	}
}
