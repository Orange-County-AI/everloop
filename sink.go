package main

// Delivery: how a drained spool message reaches the agent session.
//
// CHANNEL_SINK selects the transport:
//
//	mattermost (default) a direct message from everloop's own Mattermost
//	                     account to the recipient agent's, which that agent's
//	                     own mattermost-agents listener picks up and wakes the
//	                     session with — see mattermost_api.go
//	herdr                agent.prompt over herdr's unix socket — see
//	                     herdr_socket.go. Still fully supported: a box whose
//	                     agent herdr owns and which has no Mattermost account
//	                     sets this and nothing changes for it.
//	none                 tools-only serve: no draining, no presence. Pair with
//	                     a standalone pump that owns delivery ("tools" works
//	                     too), or use it for a dev checkout that should never
//	                     deliver.
//
// Mattermost is the default because it is the one transport that outlives the
// session it wakes. herdr delivery addresses a pane on this same box, so it
// stops working the day the agent is restarted elsewhere or under a different
// name; an account id is the agent's address for as long as the agent exists.
// The wake also lands in a conversation an operator can read back afterwards,
// which a socket write leaves no trace of.
//
// Both transports are harness-agnostic, and neither is a per-harness plane.
// herdr types the event into whatever agent is in the pane — claude, codex,
// omp, opencode, pi. Mattermost hands it to the listener that agent already
// runs to hear from people, whichever harness it is. What is deliberately
// absent is a sink that only one harness implements: an MCP notification only
// Claude Code answers, an HTTP turn only opencode accepts, a webhook only
// hermes serves. The one that existed silently dropped events on every other
// harness, so an unknown CHANNEL_SINK refuses rather than picking something: a
// value this binary does not implement must fail loudly at startup, not deliver
// into a void.
//
// Messages are wrapped in a `<channel ...meta>content</channel>` envelope,
// which is what the server's instructions text tells the agent to expect. On
// the mattermost transport that envelope is the body of the post, clipped to
// the server's post bound — see mattermostBody.
//
// THE DEFAULT CHANGED, and a config that predates the change — HERDR_TARGET
// set, CHANNEL_SINK unset — used to be complete and now is not. Such a config
// refuses at startup with both remedies named rather than falling back to
// herdr: an implicit transport chosen by whichever env var happens to be set is
// the silent guess this whole file exists to refuse.
//
// A delivery failure leaves the message claimed-but-unacked, so the spool's
// at-least-once reclaim retries it, in order, on the next poll.

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

type sink interface {
	deliver(content string, meta map[string]string) error
}

// newSink picks delivery from env. `source` is the channel name that goes in the
// envelope's `source` attribute (for this program, "everloop").
func newSink(source string) (sink, error) {
	requested := strings.ToLower(strings.TrimSpace(os.Getenv("CHANNEL_SINK")))
	switch requested {
	case "", "mattermost":
		// The recipient is required for the same reason HERDR_TARGET is:
		// everloop would otherwise have to guess an address, and a wrong guess
		// posts a firing into a stranger's inbox rather than failing.
		target := os.Getenv("MATTERMOST_TARGET")
		if target == "" {
			return nil, missingMattermostTarget(requested == "")
		}
		dest, err := parseMattermostTarget(target)
		if err != nil {
			return nil, err
		}
		// The SENDING identity, which must not be the recipient's own: a
		// listener never delivers a post its own account wrote. There is
		// deliberately no default and no fall back to MATTERMOST_AGENT_CONFIG
		// — that variable is usually already exported on these boxes, pointing
		// at the agent's own profile, and inheriting it is exactly the
		// silently-dropped delivery this sink refuses.
		profile := os.Getenv("MATTERMOST_PROFILE")
		if profile == "" {
			return nil, fmt.Errorf("CHANNEL_SINK=mattermost requires MATTERMOST_PROFILE (the path of a mattermost-agents profile for the SENDING identity — a separate automation account, never the recipient's own, whose listener would drop its own posts)")
		}
		timeoutMS := mattermostDefaultTimeoutMS
		if v := os.Getenv("MATTERMOST_POST_TIMEOUT_MS"); v != "" {
			n, err := strconv.Atoi(strings.TrimSpace(v))
			if err != nil || n < 1 {
				return nil, fmt.Errorf("invalid MATTERMOST_POST_TIMEOUT_MS %q: want a positive integer (milliseconds)", v)
			}
			timeoutMS = n
		}
		driver, err := newMattermostHTTPDriver(mattermostOptions{
			ProfilePath: profile,
			Connection:  os.Getenv("MATTERMOST_CONNECTION"),
		})
		if err != nil {
			return nil, fmt.Errorf("mattermost sink: %w", err)
		}
		// Offline half of the self-delivery guard: when the profile pins the
		// sender's user id and the target is a user id, the silent drop is
		// knowable now rather than after the first firing disappears.
		if err := driver.refuseSelfDelivery(dest); err != nil {
			return nil, err
		}
		return &mattermostSink{
			driver:  driver,
			dest:    dest,
			timeout: time.Duration(timeoutMS) * time.Millisecond,
			source:  source,
		}, nil
	case "herdr":
		target := os.Getenv("HERDR_TARGET")
		if target == "" {
			// Refuse rather than guess. A pane id would be available from
			// HERDR_PANE_ID, but a pane id dies with the pane while an agent
			// name survives a restart, so silently picking the short-lived one
			// would turn a config omission into a delivery that stops working
			// a week later.
			return nil, fmt.Errorf("CHANNEL_SINK=herdr requires HERDR_TARGET (a herdr agent target: agent name, pane id, or workspace/tab path)")
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
		driver, err := newHerdrSocketDriver(herdrSocketOptions{})
		if err != nil {
			return nil, fmt.Errorf("herdr sink: %w", err)
		}
		return &herdrSink{
			driver:  driver,
			target:  target,
			timeout: time.Duration(timeoutMS) * time.Millisecond,
			source:  source,
		}, nil
	case "none", "tools":
		// Tools-only: serve exposes the loop tools but never drains (and never
		// heartbeats presence) — something else owns delivery. Without this,
		// mounting serve beside a pump would ack messages twice.
		return nil, nil
	default:
		return nil, fmt.Errorf("unknown CHANNEL_SINK %q: want mattermost (default), herdr or none", os.Getenv("CHANNEL_SINK"))
	}
}

// missingMattermostTarget refuses, and says how to fix it. A deployment that
// predates the default change arrives here with CHANNEL_SINK unset and
// HERDR_TARGET set — a config that used to be complete — so that case gets
// both remedies by name. Quietly using herdr for it instead would be the
// implicit transport this file refuses to pick.
func missingMattermostTarget(byDefault bool) error {
	const address = "MATTERMOST_TARGET (the recipient agent: @username or its 26-character Mattermost user id)"
	switch {
	case !byDefault:
		return fmt.Errorf("CHANNEL_SINK=mattermost requires %s", address)
	case os.Getenv("HERDR_TARGET") != "":
		return fmt.Errorf("CHANNEL_SINK is unset and now defaults to mattermost, which requires %s. HERDR_TARGET is set, so this config predates the change: either add MATTERMOST_TARGET and MATTERMOST_PROFILE to move this session onto Mattermost, or set CHANNEL_SINK=herdr to keep the previous transport", address)
	default:
		return fmt.Errorf("CHANNEL_SINK is unset and defaults to mattermost, which requires %s. Set CHANNEL_SINK=herdr for the herdr transport, or CHANNEL_SINK=none for a tools-only serve", address)
	}
}

// parseMattermostTarget accepts the two recipient forms that can be validated
// before the first delivery: an @username, and a bare Mattermost user id. A
// username needs the sigil because an id is itself 26 legal username
// characters, and resolving the wrong one posts a firing to a stranger.
//
// There is deliberately no channel form. A firing is a wake addressed to one
// agent; posted in a shared channel it would wake every member on every tick,
// and a ten-minute loop in a team channel is a fleet-wide interrupt.
func parseMattermostTarget(raw string) (mattermostDest, error) {
	target := strings.TrimSpace(raw)
	if name, ok := strings.CutPrefix(target, "@"); ok {
		if name == "" {
			return mattermostDest{}, fmt.Errorf("invalid MATTERMOST_TARGET %q: no username after the @", raw)
		}
		return mattermostDest{Username: name}, nil
	}
	if isMattermostID(target) {
		return mattermostDest{UserID: target}, nil
	}
	return mattermostDest{}, fmt.Errorf("invalid MATTERMOST_TARGET %q: want @username, or a %d-character Mattermost user id", raw, mattermostIDLen)
}

// envelope renders the `<channel ...>` wrapper: every non-empty meta entry
// becomes an attribute, sorted so the same event always renders identically.
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

// --- herdr: agent.prompt over the socket -------------------------------------

// herdrSink hands the envelope to herdr, which submits it as session input to
// whichever agent occupies the target pane. No shell and no subprocess are on
// this path, so an envelope carrying `$(...)`, quotes, backticks or newlines
// travels as one literal JSON string.
type herdrSink struct {
	driver  herdrDriver
	target  string
	timeout time.Duration
	source  string
}

// herdrDefaultTimeoutMS is what the wait gets when HERDR_PROMPT_TIMEOUT_MS is
// unset: long enough for an agent to finish a real turn, short enough that a
// wedged one does not hold the drain loop past the next few polls.
const herdrDefaultTimeoutMS = 120000

func (s *herdrSink) deliver(content string, meta map[string]string) error {
	result := s.driver.promptAgent(context.Background(), s.target, envelope(s.source, content, meta), s.timeout)
	if result.OK {
		// Settled includes *blocked* (the agent stopped on a permission
		// prompt), so this is evidence the tick was delivered, not proof it was
		// acted on. Acking anyway is deliberate and everloop's design absorbs
		// it: ticks coalesce, so the next delivery carries the catch-up count
		// rather than the work being lost.
		return nil
	}
	// Anything else leaves the message claimed-but-unacked: the spool reclaims
	// it on the next poll and drainLoop stops the batch here to preserve order.
	// The code is included because it is the difference that matters —
	// `agent_not_found` means HERDR_TARGET names nobody (fix the config), while
	// a bare message means herdr could not be reached (delivery unknown).
	if result.Code != "" {
		return fmt.Errorf("herdr agent prompt %s: %s: %s", s.target, result.Code, result.Error)
	}
	return fmt.Errorf("herdr agent prompt %s: %s", s.target, result.Error)
}

// --- mattermost: a DM from everloop's own account ----------------------------

// mattermostSink posts the envelope as a direct message from everloop's own
// Mattermost account to the recipient agent's. The wake is the recipient's own
// mattermost-agents listener seeing a post somebody else wrote in a channel it
// watches — which is why the sending identity must be a separate account, and
// why the post is a DM: it reaches exactly one agent.
//
// No shell and no subprocess are on the delivery path, so an envelope carrying
// `$(...)`, quotes, backticks or newlines travels as one literal JSON string.
type mattermostSink struct {
	driver  mattermostDriver
	dest    mattermostDest
	timeout time.Duration
	source  string
}

// mattermostDefaultTimeoutMS is what a delivery gets when
// MATTERMOST_POST_TIMEOUT_MS is unset. It covers the whole operation, not one
// request: on the first firing that is a token resolution (which can mean a
// `secret` lookup against a password manager), a `/users/me`, possibly a
// username lookup, a direct-channel create and the post. Every one of those
// but the post is cached afterwards.
const mattermostDefaultTimeoutMS = 30000

func (s *mattermostSink) deliver(content string, meta map[string]string) error {
	result := s.driver.post(context.Background(), s.dest, mattermostBody(s.source, content, meta), s.timeout)
	if result.OK {
		// The post exists, so the recipient's listener will see it on its next
		// poll. Like the herdr sink this is evidence of delivery, not proof the
		// agent acted, and acking anyway is deliberate: ticks coalesce, so the
		// next firing carries the catch-up count rather than the work being
		// lost.
		return nil
	}
	// Anything else leaves the message claimed-but-unacked: the spool reclaims
	// it on the next poll and drainLoop stops the batch here to preserve order.
	// The code is the difference that matters — Mattermost's own error id
	// (`store.sql_user.missing_account.const` for a recipient who does not
	// exist, `api.context.permissions.app_error` for a token that may not
	// address them) says the config is wrong, while a bare message means the
	// server could not be reached and the outcome is unknown.
	if result.Code != "" {
		return fmt.Errorf("mattermost post to %s: %s: %s", s.dest, result.Code, result.Error)
	}
	return fmt.Errorf("mattermost post to %s: %s", s.dest, result.Error)
}
