package main

// Command loops: a loop that runs a command on each firing and spools an event
// only when the command has something to say.
//
// A static loop is a heartbeat — it says the same thing every interval whether
// or not anything happened. A command loop is a WATCH: check something, stay
// silent if nothing changed, wake the session only when it did. Silence is the
// feature; everything here exists to protect it (damped failures, bounded
// output) or to protect the data it carries (accumulating coalesce).
//
// The command runs under the OS timer's environment, NOT a login shell —
// systemd's user manager on Linux, launchd on macOS. `~/.profile` is never
// sourced. See README "Command loops" for the caveat that bites.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

const (
	defaultCommandTimeout = 60 * time.Second
	maxCommandTimeout     = time.Hour

	// Output bounds. An outage plus a chatty command must not grow the spool
	// without limit, and one run that dumps a core file must not become the
	// event. Both losses are made visible in the body rather than silent.
	maxRunBytes   = 16 << 10
	maxEventBytes = 64 << 10
	maxEventRuns  = 20
)

// parseTimeout bounds a single command execution. The default is deliberately
// well under any sane loop interval: a command that outlives its own interval
// piles ticks up behind it.
func parseTimeout(s string) (time.Duration, error) {
	if s == "" {
		return defaultCommandTimeout, nil
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		return 0, fmt.Errorf("invalid timeout %q: use forms like 30s, 2m", s)
	}
	if d < time.Second || d > maxCommandTimeout {
		return 0, fmt.Errorf("timeout %q out of range: 1s..1h", s)
	}
	return d, nil
}

// runResult is one execution of a loop's command.
type runResult struct {
	Stdout   string
	Stderr   string
	Exit     int
	TimedOut bool
	Err      error // could not run at all (no /bin/sh, fork failure) — not a command failure
}

func (r runResult) failed() bool { return r.Err != nil || r.TimedOut || r.Exit != 0 }

func (r runResult) status() string {
	if r.TimedOut {
		return "timeout"
	}
	return "error"
}

// runCommand executes the loop's command under `sh -c` with a bounded lifetime.
// The child gets its own process group and the timeout kills the group, not
// just the shell: a hung `curl` inside a pipeline would otherwise survive the
// kill, keep the captured pipe open, and wedge the tick that was supposed to
// bound it. WaitDelay is the backstop for a grandchild that ignores SIGKILL's
// consequences (e.g. one that has already re-parented).
func runCommand(command string, timeout time.Duration) runResult {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, "sh", "-c", command)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error { return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) }
	cmd.WaitDelay = 5 * time.Second
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr

	err := cmd.Run()
	res := runResult{Stdout: stdout.String(), Stderr: stderr.String()}
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		res.TimedOut = true
		return res
	}
	if err != nil {
		var ee *exec.ExitError
		if errors.As(err, &ee) {
			res.Exit = ee.ExitCode()
		} else {
			res.Err = err
		}
	}
	return res
}

// clip bounds captured output at a byte budget. Cutting mid-rune would produce
// invalid UTF-8 in the event body, so the tail is scrubbed rather than trusted.
func clip(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return strings.ToValidUTF8(s[:max], "") +
		fmt.Sprintf("\n[everloop] output truncated, %d bytes dropped]", len(s)-max)
}

// --- failure damping ---------------------------------------------------------

func stateDir() string { return filepath.Join(dataDir(), "state") }

// runState is a loop's damping memory. It lives beside the loop rather than
// inside it because a tick must not rewrite the loop definition: that would
// race `update` and churn UpdatedAt on every single firing.
type runState struct {
	Consecutive  int       `json:"consecutive_failures"`
	LastReported int       `json:"last_reported_failure"` // 0 = nothing reported since last success
	UpdatedAt    time.Time `json:"updated_at"`
}

func runStatePath(name string) string { return filepath.Join(stateDir(), name+".json") }

// loadRunState treats any unreadable state as "healthy". Losing the damping
// memory costs one extra failure report, which is the safe direction to fail.
func loadRunState(name string) runState {
	var st runState
	if data, err := os.ReadFile(runStatePath(name)); err == nil {
		json.Unmarshal(data, &st)
	}
	return st
}

func saveRunState(name string, st runState) error { return writeJSON(runStatePath(name), &st) }

// shouldReport damps a permanently broken command down to a trickle: report
// the 1st, 2nd, 4th, 8th, 16th ... consecutive failure. everloop's whole value
// is silence when nothing is happening, and a watch that pages the session
// every interval forever is worse than no watch at all — it trains you to
// ignore it. Doubling still reports promptly when a break is new.
func shouldReport(consecutive int) bool {
	return consecutive > 0 && consecutive&(consecutive-1) == 0
}

// nextReportAt is the streak length at which the next report will fire, so a
// damped failure event can say when to expect the next one.
func nextReportAt(consecutive int) int {
	n := 1
	for n <= consecutive {
		n <<= 1
	}
	return n
}

// --- tick --------------------------------------------------------------------

// enqueueCommandTick runs the loop's command and spools an event only if it
// produced something worth waking the session for.
//
// The command runs OUTSIDE the queue lock. It may take up to its full timeout,
// and holding the flock that long would stall the drain loop and every other
// loop's tick — one slow watch would become everyone's problem.
func enqueueCommandTick(l *Loop) error {
	timeout, err := parseTimeout(l.Timeout)
	if err != nil {
		return err
	}
	res := runCommand(l.Command, timeout)

	return withQueueLock(func() error {
		st := loadRunState(l.Name)
		now := time.Now().UTC()
		var runs []runOutput

		if res.failed() {
			st.Consecutive++
			if shouldReport(st.Consecutive) {
				st.LastReported = st.Consecutive
				runs = append(runs, runOutput{
					At:     now,
					Status: res.status(),
					Exit:   res.Exit,
					Text:   failureText(l, res, timeout, st.Consecutive),
				})
			}
		} else {
			if st.LastReported > 0 {
				// Recovery is worth exactly one event: the session was told the
				// watch was broken and would otherwise never learn it isn't.
				runs = append(runs, runOutput{
					At:     now,
					Status: "recovered",
					Text: fmt.Sprintf("[everloop] loop %q recovered after %d consecutive failures.",
						l.Name, st.Consecutive),
				})
			}
			st.Consecutive, st.LastReported = 0, 0
			if out := strings.TrimRight(res.Stdout, "\n"); out != "" {
				runs = append(runs, runOutput{At: now, Text: clip(out, maxRunBytes)})
			}
		}

		st.UpdatedAt = now
		if err := saveRunState(l.Name, st); err != nil {
			return err
		}
		if len(runs) == 0 {
			return nil // exit 0, no output: the point of the exercise. Stay silent.
		}
		return appendTickRuns(l, runs, now)
	})
}

// failureText renders one failed run. stderr is included because that is where
// a broken environment announces itself (`env: 'uv': No such file or
// directory`) — a failure event that dropped it would reproduce the historical
// bug where a timer failed invisibly for days.
func failureText(l *Loop, res runResult, timeout time.Duration, consecutive int) string {
	var b strings.Builder
	switch {
	case res.Err != nil:
		fmt.Fprintf(&b, "[everloop] loop %q could not run its command: %v", l.Name, res.Err)
	case res.TimedOut:
		fmt.Fprintf(&b, "[everloop] loop %q command timed out after %s", l.Name, timeout)
	default:
		fmt.Fprintf(&b, "[everloop] loop %q command failed (exit %d)", l.Name, res.Exit)
	}
	fmt.Fprintf(&b, " — consecutive failure %d; damped, next report at %d.\n", consecutive, nextReportAt(consecutive))
	fmt.Fprintf(&b, "$ %s\n", l.Command)
	if s := strings.TrimSpace(res.Stderr); s != "" {
		b.WriteString(clip(s, maxRunBytes) + "\n")
	}
	if s := strings.TrimSpace(res.Stdout); s != "" {
		b.WriteString(clip(s, maxRunBytes) + "\n")
	}
	return strings.TrimRight(b.String(), "\n")
}

// appendTickRuns ACCUMULATES into the loop's single pending-tick slot instead
// of overwriting it.
//
// Overwriting is correct for a static loop: the payload is identical every
// time, so five firings collapsing into one lose nothing. For a command loop
// every firing carries DIFFERENT output, and overwriting would mean that an
// outage during four cover changes delivers only the fourth — a watch that
// lies, which is worse than no watch. Per-firing spool files (what
// enqueueMessage does) would also preserve the data, but they reintroduce
// exactly the flood that coalescing exists to prevent: reconnect after a day
// down and the session eats 144 separate events. Accumulating keeps both
// properties — one event per loop, every firing's output preserved in order —
// at the cost of a bound (see boundRuns) so the file cannot grow forever.
//
// The claim-during-tick invariant is unchanged: claimPending renames the slot
// under this same lock before reading it, so a tick landing mid-claim finds no
// file and starts a fresh accumulation. Nothing merges into an event already in
// flight and nothing is dropped.
func appendTickRuns(l *Loop, runs []runOutput, now time.Time) error {
	path := filepath.Join(queueDir(), "tick-"+l.Name+".json")
	msg := QueueMsg{ID: randomID(), Kind: "tick", Loop: l.Name, FirstAt: now}
	if data, err := os.ReadFile(path); err == nil {
		var prev QueueMsg
		if json.Unmarshal(data, &prev) == nil {
			msg.ID, msg.FirstAt = prev.ID, prev.FirstAt
			msg.Runs, msg.Dropped = prev.Runs, prev.Dropped
		}
	}
	msg.Runs = append(msg.Runs, runs...)
	msg.Runs, msg.Dropped = boundRuns(msg.Runs, msg.Dropped)
	msg.LastAt = now
	msg.Count = len(msg.Runs)
	msg.Content = renderRuns(l.Message, msg.Runs, msg.Dropped)
	return writeJSON(path, &msg)
}

// boundRuns caps an accumulating event by dropping the OLDEST runs. Something
// has to give when a chatty command meets a long outage; dropping the oldest
// keeps the most actionable state, and the count is carried into the body so
// the loss is stated rather than silently pretended away.
func boundRuns(runs []runOutput, dropped int) ([]runOutput, int) {
	total := 0
	for _, r := range runs {
		total += len(r.Text)
	}
	for len(runs) > 1 && (len(runs) > maxEventRuns || total > maxEventBytes) {
		total -= len(runs[0].Text)
		runs = runs[1:]
		dropped++
	}
	return runs, dropped
}

// renderRuns composes the delivered body: the loop's Message as a standing
// preamble, then each accumulated run in firing order.
//
// Message is a PREAMBLE rather than being ignored when Command is set, because
// the two carry different things: the command supplies the facts, the message
// supplies the standing instruction about them ("spawn a subagent to judge
// this"). Ignoring Message would force that instruction into the command's own
// output, i.e. into whatever script the user happens to be wrapping. It is
// omitted from pure-failure events, where "judge this" applied to a stack
// trace is nonsense.
func renderRuns(preamble string, runs []runOutput, dropped int) string {
	hasOutput := false
	for _, r := range runs {
		if r.Status == "" {
			hasOutput = true
		}
	}
	var b strings.Builder
	if preamble != "" && hasOutput {
		b.WriteString(preamble + "\n")
	}
	if dropped > 0 {
		fmt.Fprintf(&b, "\n[everloop] %d earlier run(s) dropped to bound the spool.\n", dropped)
	}
	for i, r := range runs {
		switch {
		case len(runs) > 1:
			fmt.Fprintf(&b, "\n[everloop] run %d of %d — %s\n", i+1, len(runs), r.At.Format(time.RFC3339))
		case b.Len() > 0:
			b.WriteString("\n")
		}
		b.WriteString(r.Text + "\n")
	}
	return strings.TrimRight(b.String(), "\n")
}
