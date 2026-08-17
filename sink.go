package main

// Delivery: how a drained spool message reaches the agent session.
//
// There is ONE transport, herdr's socket API, because herdr already knows every
// harness it drives. It types the event into whatever agent is in the pane —
// claude, codex, omp, opencode, pi — so the event arrives as ordinary session
// input and everloop needs no per-harness plane of its own. That is what makes
// `serve` harness-agnostic: not a menu of sinks, but a transport that does not
// care which model is on the other end.
//
// CHANNEL_SINK selects between delivering and not delivering:
//
//	herdr  (default) agent.prompt over herdr's unix socket — see herdr_socket.go
//	none             tools-only serve: no draining, no presence. Pair with a
//	                 standalone pump that owns delivery ("tools" works too), or
//	                 use it for a dev checkout that should never deliver.
//
// There is deliberately no second sink to fall back on. A per-harness plane —
// an MCP notification only Claude Code implements, an HTTP turn only opencode
// accepts, a webhook only hermes answers — is another way to reach an agent
// herdr can already reach, with its own credentials and its own liveness story,
// and the one that existed silently dropped events on every other harness. So
// an unknown CHANNEL_SINK refuses rather than picking something: a value this
// binary does not implement must fail loudly at startup, not deliver into a
// void.
//
// Messages are wrapped in a `<channel ...meta>content</channel>` envelope, which
// is what the server's instructions text tells the agent to expect. A delivery
// failure leaves the message claimed-but-unacked, so the spool's at-least-once
// reclaim retries it, in order, on the next poll.

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
// envelope ("tincan" / "everloop").
func newSink(source string) (sink, error) {
	switch strings.ToLower(os.Getenv("CHANNEL_SINK")) {
	case "", "herdr":
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
		return nil, fmt.Errorf("unknown CHANNEL_SINK %q: want herdr (default) or none", os.Getenv("CHANNEL_SINK"))
	}
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
