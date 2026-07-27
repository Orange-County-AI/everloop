package main

// Command-loop tests. The load-bearing one is
// TestCommandTicksAccumulateAcrossAnOutage: coalescing that overwrites is
// invisible until a session is down, which is exactly when losing firings
// matters. The rest guard the properties that make a watch trustworthy —
// silence when nothing changed, a bounded spool, damped failures, and a
// timeout that actually kills the tree.

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// newTestLoop points everloop at a scratch data dir and writes a loop
// definition straight to it: tests must never install real OS timers.
func newTestLoop(t *testing.T, l Loop) *Loop {
	t.Helper()
	t.Setenv("EVERLOOP_DATA_DIR", t.TempDir())
	if l.Name == "" {
		l.Name = "watch"
	}
	l.Enabled = true
	if err := saveLoop(&l); err != nil {
		t.Fatal(err)
	}
	return &l
}

func pendingTick(t *testing.T, name string) (QueueMsg, bool) {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(queueDir(), "tick-"+name+".json"))
	if err != nil {
		return QueueMsg{}, false
	}
	var m QueueMsg
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatalf("corrupt spool file: %v", err)
	}
	return m, true
}

func queueEntries(t *testing.T) []string {
	t.Helper()
	entries, err := os.ReadDir(queueDir())
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, e := range entries {
		names = append(names, e.Name())
	}
	return names
}

func tick(t *testing.T, name string) {
	t.Helper()
	if err := enqueueTick(name); err != nil {
		t.Fatal(err)
	}
}

// countingCommand prints a different line on every firing, which is what makes
// overwrite-vs-accumulate observable at all.
func countingCommand(t *testing.T) string {
	t.Helper()
	counter := filepath.Join(t.TempDir(), "n")
	return fmt.Sprintf(`n=$(cat %[1]s 2>/dev/null || echo 0); n=$((n+1)); echo $n >%[1]s; echo "change-$n"`, counter)
}

func TestCommandLoopStaysSilentWhenNothingChanged(t *testing.T) {
	l := newTestLoop(t, Loop{Command: "exit 0", Every: "1m"})
	for range 3 {
		tick(t, l.Name)
	}
	if names := queueEntries(t); len(names) != 0 {
		t.Fatalf("silent command spooled %v, want an empty queue", names)
	}
}

// stdout decides; a command that only chatters on stderr is still silent.
func TestCommandLoopIgnoresStderrOnSuccess(t *testing.T) {
	l := newTestLoop(t, Loop{Command: "echo noise >&2; exit 0", Every: "1m"})
	tick(t, l.Name)
	if names := queueEntries(t); len(names) != 0 {
		t.Fatalf("stderr-only run spooled %v, want an empty queue", names)
	}
}

func TestCommandLoopSpoolsOutputUnderMessagePreamble(t *testing.T) {
	l := newTestLoop(t, Loop{
		Message: "Judge whether this change was deliberate.",
		Command: `echo "LUMA COVER-CHANGED evt-1"`,
		Every:   "1m",
	})
	tick(t, l.Name)

	msg, ok := pendingTick(t, l.Name)
	if !ok {
		t.Fatal("command with output spooled nothing")
	}
	want := "Judge whether this change was deliberate.\n\nLUMA COVER-CHANGED evt-1"
	if msg.Content != want {
		t.Fatalf("content = %q, want %q", msg.Content, want)
	}
	if msg.Count != 1 || msg.Kind != "tick" || msg.Loop != l.Name {
		t.Fatalf("unexpected envelope: %+v", msg)
	}
	if msg.status() != "" {
		t.Fatalf("status = %q, want healthy", msg.status())
	}
}

// The one that matters. A session down across four distinct firings must
// receive all four, in order, as ONE event — not the last one wearing a
// coalesced_count of 4, and not four separate events flooding the reconnect.
func TestCommandTicksAccumulateAcrossAnOutage(t *testing.T) {
	l := newTestLoop(t, Loop{Message: "standing instruction", Command: countingCommand(t), Every: "1m"})
	for range 4 {
		tick(t, l.Name)
	}

	if names := queueEntries(t); len(names) != 1 {
		t.Fatalf("queue holds %v, want exactly one coalesced event", names)
	}
	msg, ok := pendingTick(t, l.Name)
	if !ok {
		t.Fatal("nothing spooled")
	}
	if msg.Count != 4 || len(msg.Runs) != 4 {
		t.Fatalf("count=%d runs=%d, want 4 firings preserved", msg.Count, len(msg.Runs))
	}
	if msg.Dropped != 0 {
		t.Fatalf("dropped %d runs under the bound", msg.Dropped)
	}
	for i := 1; i <= 4; i++ {
		want := fmt.Sprintf("change-%d", i)
		if !strings.Contains(msg.Content, want) {
			t.Fatalf("firing %d lost: %q missing from\n%s", i, want, msg.Content)
		}
	}
	// Order is the other half of the contract: a diff stream read backwards is
	// as wrong as a diff stream truncated.
	var at []int
	for i := 1; i <= 4; i++ {
		at = append(at, strings.Index(msg.Content, fmt.Sprintf("change-%d", i)))
	}
	for i := 1; i < len(at); i++ {
		if at[i] < at[i-1] {
			t.Fatalf("runs out of firing order: offsets %v in\n%s", at, msg.Content)
		}
	}
	if !strings.HasPrefix(msg.Content, "standing instruction\n") {
		t.Fatalf("preamble should appear once, at the top:\n%s", msg.Content)
	}
	if strings.Count(msg.Content, "standing instruction") != 1 {
		t.Fatalf("preamble repeated per run:\n%s", msg.Content)
	}
}

// A tick that lands mid-claim must start a fresh event rather than merge into
// one already in flight — the invariant claimPending's rename-first exists for.
func TestCommandTickDuringClaimStartsAFreshEvent(t *testing.T) {
	cmd := countingCommand(t)
	l := newTestLoop(t, Loop{Command: cmd, Every: "1m"})
	tick(t, l.Name)
	tick(t, l.Name)

	claimed, err := claimPending()
	if err != nil {
		t.Fatal(err)
	}
	if len(claimed) != 1 || claimed[0].msg.Count != 2 {
		t.Fatalf("claimed %d messages (first count=%d), want 1 holding 2 runs", len(claimed), claimed[0].msg.Count)
	}

	tick(t, l.Name)
	tick(t, l.Name)
	fresh, ok := pendingTick(t, l.Name)
	if !ok {
		t.Fatal("post-claim firings spooled nothing")
	}
	if fresh.Count != 2 {
		t.Fatalf("fresh event count = %d, want 2", fresh.Count)
	}
	if fresh.ID == claimed[0].msg.ID {
		t.Fatal("fresh event reused the claimed event's ID")
	}
	// Nothing merged backwards into the in-flight message, and nothing was lost:
	// firings 1-2 are claimed, 3-4 are pending, all four exist exactly once.
	all := claimed[0].msg.Content + "\n" + fresh.Content
	for i := 1; i <= 4; i++ {
		if n := strings.Count(all, fmt.Sprintf("change-%d", i)); n != 1 {
			t.Fatalf("firing %d appears %d times across claimed+pending", i, n)
		}
	}
	if strings.Contains(claimed[0].msg.Content, "change-3") {
		t.Fatal("a post-claim firing merged into the in-flight event")
	}
}

// Backwards compatibility is non-negotiable: a loop with no command must
// behave exactly as before — one slot, overwritten, Count bumped.
func TestStaticLoopCoalescingIsUnchanged(t *testing.T) {
	l := newTestLoop(t, Loop{Message: "Reconcile the ledger.", Every: "1m"})
	for range 5 {
		tick(t, l.Name)
	}
	if names := queueEntries(t); len(names) != 1 {
		t.Fatalf("queue holds %v, want one coalesced tick", names)
	}
	msg, _ := pendingTick(t, l.Name)
	if msg.Content != "Reconcile the ledger." || msg.Count != 5 {
		t.Fatalf("content=%q count=%d, want the static message with count 5", msg.Content, msg.Count)
	}
	if len(msg.Runs) != 0 {
		t.Fatalf("static tick grew %d runs", len(msg.Runs))
	}
}

// A permanently broken command must not page the session every interval
// forever: report the 1st, 2nd, 4th, 8th... and exactly one recovery.
func TestCommandFailuresAreDampedAndRecoveryReportedOnce(t *testing.T) {
	flagDir := t.TempDir()
	fail := filepath.Join(flagDir, "fail")
	l := newTestLoop(t, Loop{
		Message: "standing instruction",
		Command: fmt.Sprintf(`if [ -e %s ]; then echo "env: 'uv': No such file or directory" >&2; exit 127; fi; echo back-to-normal`, fail),
		Every:   "1m",
	})
	if err := os.WriteFile(fail, nil, 0o644); err != nil {
		t.Fatal(err)
	}

	var reports []int // runs present after each consecutive failure
	for i := 1; i <= 8; i++ {
		tick(t, l.Name)
		msg, _ := pendingTick(t, l.Name)
		reports = append(reports, len(msg.Runs))
	}
	// Cumulative report count after failures 1..8 — new report at 1,2,4,8 only.
	want := []int{1, 2, 2, 3, 3, 3, 3, 4}
	for i := range want {
		if reports[i] != want[i] {
			t.Fatalf("reports after each failure = %v, want %v (damping is off)", reports, want)
		}
	}

	msg, _ := pendingTick(t, l.Name)
	if msg.status() != "error" {
		t.Fatalf("status = %q, want error", msg.status())
	}
	if !strings.Contains(msg.Content, "env: 'uv': No such file or directory") {
		t.Fatalf("stderr dropped from the failure report, the exact historical bug:\n%s", msg.Content)
	}
	if !strings.Contains(msg.Content, "exit 127") {
		t.Fatalf("exit status missing:\n%s", msg.Content)
	}
	if strings.Contains(msg.Content, "standing instruction") {
		t.Fatalf("preamble applied to a pure-failure event:\n%s", msg.Content)
	}

	// Recovery: one notice, plus the output the healthy run produced.
	os.Remove(fail)
	tick(t, l.Name)
	msg, _ = pendingTick(t, l.Name)
	if !strings.Contains(msg.Content, "recovered after 8 consecutive failures") {
		t.Fatalf("no recovery notice:\n%s", msg.Content)
	}
	if !strings.Contains(msg.Content, "back-to-normal") {
		t.Fatalf("recovery run's own output dropped:\n%s", msg.Content)
	}
	if msg.status() != "" {
		t.Fatalf("status = %q after recovery, want healthy", msg.status())
	}

	// Recovery is reported once, not on every subsequent healthy firing.
	before := len(msg.Runs)
	tick(t, l.Name)
	msg, _ = pendingTick(t, l.Name)
	if got := len(msg.Runs) - before; got != 1 {
		t.Fatalf("healthy firing added %d runs, want 1 (recovery repeated?)", got)
	}
	if strings.Count(msg.Content, "recovered after") != 1 {
		t.Fatalf("recovery notice repeated:\n%s", msg.Content)
	}
}

func TestShouldReportDoublesTheGap(t *testing.T) {
	var reported []int
	for i := 1; i <= 40; i++ {
		if shouldReport(i) {
			reported = append(reported, i)
		}
	}
	want := []int{1, 2, 4, 8, 16, 32}
	if fmt.Sprint(reported) != fmt.Sprint(want) {
		t.Fatalf("reported at %v, want %v", reported, want)
	}
	if n := nextReportAt(5); n != 8 {
		t.Fatalf("nextReportAt(5) = %d, want 8", n)
	}
}

// A hung command must not wedge the tick, and the kill must reach children the
// shell spawned — otherwise the captured pipe stays open and Run blocks well
// past the timeout.
func TestCommandTimeoutKillsTheProcessTree(t *testing.T) {
	l := newTestLoop(t, Loop{Command: "sleep 30 & wait", Timeout: "1s", Every: "1m"})
	start := time.Now()
	tick(t, l.Name)
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Fatalf("tick took %s for a 1s timeout: the process tree outlived the kill", elapsed)
	}
	msg, ok := pendingTick(t, l.Name)
	if !ok {
		t.Fatal("timeout spooled nothing")
	}
	if msg.status() != "timeout" {
		t.Fatalf("status = %q, want timeout (distinct from a plain failure)", msg.status())
	}
	if !strings.Contains(msg.Content, "timed out after 1s") {
		t.Fatalf("body does not name the timeout:\n%s", msg.Content)
	}
}

// An outage plus a chatty command must not spool an unbounded file.
func TestAccumulationIsBounded(t *testing.T) {
	l := newTestLoop(t, Loop{Command: countingCommand(t), Every: "1m"})
	for range maxEventRuns + 12 {
		tick(t, l.Name)
	}
	msg, _ := pendingTick(t, l.Name)
	if len(msg.Runs) != maxEventRuns {
		t.Fatalf("kept %d runs, want the %d-run bound", len(msg.Runs), maxEventRuns)
	}
	if msg.Dropped != 12 {
		t.Fatalf("dropped = %d, want 12", msg.Dropped)
	}
	if !strings.Contains(msg.Content, "12 earlier run(s) dropped") {
		t.Fatalf("loss not stated in the body:\n%s", msg.Content[:200])
	}
	// The newest firings are the ones kept.
	if !strings.Contains(msg.Content, "change-32") || strings.Contains(msg.Content, "change-1\n") {
		t.Fatalf("bound dropped the wrong end")
	}
}

func TestOversizedRunIsClippedNotSpooledWhole(t *testing.T) {
	l := newTestLoop(t, Loop{Command: "head -c 200000 /dev/zero | tr '\\0' 'x'", Every: "1m"})
	tick(t, l.Name)
	msg, ok := pendingTick(t, l.Name)
	if !ok {
		t.Fatal("nothing spooled")
	}
	if len(msg.Content) > maxRunBytes+200 {
		t.Fatalf("content is %d bytes, want it clipped near %d", len(msg.Content), maxRunBytes)
	}
	if !strings.Contains(msg.Content, "output truncated") {
		t.Fatalf("truncation not stated in the body")
	}
}

func TestBoundedTotalBytesAcrossRuns(t *testing.T) {
	l := newTestLoop(t, Loop{Command: "head -c 12000 /dev/zero | tr '\\0' 'y'", Every: "1m"})
	for range 10 {
		tick(t, l.Name)
	}
	msg, _ := pendingTick(t, l.Name)
	if len(msg.Content) > maxEventBytes+2000 {
		t.Fatalf("event grew to %d bytes, want the %d-byte bound", len(msg.Content), maxEventBytes)
	}
	if msg.Dropped == 0 {
		t.Fatal("byte bound never engaged")
	}
}

func TestDeleteLoopClearsDampingMemory(t *testing.T) {
	l := newTestLoop(t, Loop{Command: "exit 1", Every: "1m"})
	tick(t, l.Name)
	if st := loadRunState(l.Name); st.Consecutive != 1 {
		t.Fatalf("consecutive = %d, want 1", st.Consecutive)
	}
	if err := os.Remove(runStatePath(l.Name)); err != nil {
		t.Fatal(err)
	}
	if st := loadRunState(l.Name); st.Consecutive != 0 {
		t.Fatal("state survived removal")
	}
}

// A disabled command loop must not run its command at all — the timer is the
// only thing stopped on some paths, so tick has to check too.
func TestDisabledCommandLoopDoesNotRun(t *testing.T) {
	dir := t.TempDir()
	touched := filepath.Join(dir, "ran")
	l := newTestLoop(t, Loop{Command: fmt.Sprintf("touch %s; echo hi", touched), Every: "1m"})
	l.Enabled = false
	if err := saveLoop(l); err != nil {
		t.Fatal(err)
	}
	tick(t, l.Name)
	if _, err := os.Stat(touched); err == nil {
		t.Fatal("disabled loop executed its command")
	}
}

func TestLoopSpecValidation(t *testing.T) {
	s := func(v string) *string { return &v }

	// A command loop needs no message.
	l := &Loop{}
	if err := (loopSpec{Command: s("check.sh"), Every: s("5m")}).apply(l); err != nil {
		t.Fatalf("command-only loop rejected: %v", err)
	}
	// Neither message nor command is not a loop at all.
	if err := (loopSpec{Every: s("5m")}).apply(&Loop{}); err == nil {
		t.Fatal("loop with neither message nor command accepted")
	}
	// Clearing the command drops the now-meaningless timeout, and needs a message.
	l = &Loop{Message: "hi", Command: "check.sh", Timeout: "30s", Every: "5m"}
	if err := (loopSpec{Command: s("")}).apply(l); err != nil {
		t.Fatal(err)
	}
	if l.Command != "" || l.Timeout != "" {
		t.Fatalf("clear left command=%q timeout=%q", l.Command, l.Timeout)
	}
	if err := (loopSpec{Command: s(""), Message: s("")}).apply(&Loop{Command: "x", Every: "5m"}); err == nil {
		t.Fatal("clearing both message and command accepted")
	}
	// Timeout range.
	if err := (loopSpec{Command: s("x"), Timeout: s("9h"), Every: s("5m")}).apply(&Loop{}); err == nil {
		t.Fatal("out-of-range timeout accepted")
	}
	if d, err := parseTimeout(""); err != nil || d != defaultCommandTimeout {
		t.Fatalf("default timeout = %v, %v", d, err)
	}
}
