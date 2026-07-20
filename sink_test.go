package main

// Sink tests: envelope rendering, opencode session resolve + prompt_async
// injection, hermes V2 HMAC signing + idempotency header.

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
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
