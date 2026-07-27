package main

// Scheduler tests. The load-bearing one is
// TestCatchUpFiresOnceForManyMissedIntervals: a container that was off for
// hours must not wake its session with hours of backlog, and the difference
// between "fires once" and "fires 120 times" is invisible until the day
// something actually goes down. The rest guard the properties that make the
// daemon safe to supervise — no double fires, no overlapping runs, no loop left
// unscheduled, and a crash that costs a repeat rather than a silent skip.

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

// testScheduler builds a scheduler that fires for real (so the assertions run
// against the actual spool) but logs into a buffer instead of stdout.
func testScheduler(t *testing.T) (*scheduler, *strings.Builder) {
	t.Helper()
	var mu sync.Mutex
	var logs strings.Builder
	s := newScheduler()
	s.logf = func(format string, a ...any) {
		mu.Lock()
		defer mu.Unlock()
		fmt.Fprintf(&logs, format+"\n", a...)
	}
	return s, &logs
}

// arm writes a loop's next fire directly, which is how a test puts the clock
// where it wants it without waiting.
func arm(t *testing.T, name string, next time.Time) {
	t.Helper()
	if err := saveSchedState(schedState{Name: name, NextRunAt: next}); err != nil {
		t.Fatal(err)
	}
}

// runPass fires everything due and waits for it, so assertions see a settled
// spool rather than a race.
func runPass(s *scheduler, now time.Time) {
	s.pass(now)
	s.wg.Wait()
}

func TestSchedulerFiresWhatIsDueAndLeavesTheRestAlone(t *testing.T) {
	l := newTestLoop(t, Loop{Name: "sweep", Message: "sweep the inbox", Every: "1m"})
	arm(t, l.Name, time.Now().Add(-time.Second))

	s, _ := testScheduler(t)
	runPass(s, time.Now())

	msg, ok := pendingTick(t, l.Name)
	if !ok {
		t.Fatal("a due loop did not fire")
	}
	if msg.Content != l.Message {
		t.Fatalf("spooled %q, want the loop's message", msg.Content)
	}
	st := armed(t, l.Name)
	if !st.NextRunAt.After(time.Now()) {
		t.Fatalf("next fire %s is not in the future; the loop will spin", st.NextRunAt)
	}
	if st.Fires != 1 || st.LastRunAt.IsZero() {
		t.Fatalf("fire history not recorded: %+v", st)
	}

	// A second pass right away must do nothing: the loop is no longer due.
	runPass(s, time.Now())
	if msg, _ := pendingTick(t, l.Name); msg.Count != 1 {
		t.Fatalf("loop fired again while not due (count %d)", msg.Count)
	}
}

// systemd's Persistent=true fires a missed calendar event ONCE at next boot.
// This is that, for a container that was stopped: 120 intervals go by with
// nothing running, and the session gets one tick — which is exactly what its
// instructions promise (coalesced_count says catch up once, not repeat N times).
func TestCatchUpFiresOnceForManyMissedIntervals(t *testing.T) {
	l := newTestLoop(t, Loop{Name: "sweep", Message: "sweep the inbox", Every: "1m"})
	due := time.Now().Add(-2 * time.Hour)
	arm(t, l.Name, due)

	s, logs := testScheduler(t)
	now := time.Now()
	runPass(s, now)

	msg, ok := pendingTick(t, l.Name)
	if !ok {
		t.Fatal("the missed loop did not catch up at all")
	}
	if msg.Count != 1 {
		t.Fatalf("catch-up spooled %d ticks; a missed window must fire exactly once", msg.Count)
	}
	if !strings.Contains(logs.String(), "catch-up") || !strings.Contains(logs.String(), "missed 121 fires") {
		t.Fatalf("catch-up was not reported in the log:\n%s", logs.String())
	}

	// And it resumes on the normal cadence rather than working through the
	// backlog on subsequent passes.
	st := armed(t, l.Name)
	if d := st.NextRunAt.Sub(now); d < 55*time.Second || d > 65*time.Second {
		t.Fatalf("after catch-up the next fire is %s away, want ~1m", d)
	}
	if st.CatchUps != 1 {
		t.Fatalf("catch-up not recorded in state: %+v", st)
	}
	runPass(s, time.Now())
	if msg, _ := pendingTick(t, l.Name); msg.Count != 1 {
		t.Fatalf("the backlog kept firing: count %d", msg.Count)
	}
}

// The daemon dying between spooling a tick and recording it must cost a repeat,
// not a silent skip: the spool coalesces the repeat away, whereas a skipped
// sweep is the failure everloop exists to prevent. This simulates the crash by
// firing without letting the state write happen.
func TestACrashBetweenFireAndRecordRefiresRatherThanSkipping(t *testing.T) {
	l := newTestLoop(t, Loop{Name: "sweep", Message: "sweep the inbox", Every: "1m"})
	due := time.Now().Add(-time.Second)
	arm(t, l.Name, due)

	if err := enqueueTick(l.Name); err != nil { // the tick landed...
		t.Fatal(err)
	}
	// ...and the process died here, before saveSchedState.

	s, _ := testScheduler(t)
	runPass(s, time.Now())

	msg, ok := pendingTick(t, l.Name)
	if !ok {
		t.Fatal("nothing pending after the restart")
	}
	if msg.Count != 2 {
		t.Fatalf("coalesced count %d, want 2 (the repeat folded into the pending tick)", msg.Count)
	}
	if st := armed(t, l.Name); !st.NextRunAt.After(time.Now()) {
		t.Fatal("the loop is still overdue after the recovery fire")
	}
}

func TestDisabledLoopIsNeverFired(t *testing.T) {
	l := newTestLoop(t, Loop{Name: "sweep", Message: "sweep", Every: "1m"})
	l.Enabled = false
	if err := saveLoop(l); err != nil {
		t.Fatal(err)
	}
	arm(t, l.Name, time.Now().Add(-time.Hour))

	s, _ := testScheduler(t)
	runPass(s, time.Now())

	if _, ok := pendingTick(t, l.Name); ok {
		t.Fatal("a disabled loop fired")
	}
}

// A loop the daemon has never seen — created under a different backend, or
// whose state was lost — is scheduled forward, not fired. Firing on adoption
// would mean every loop fires at once on the first start after a migration.
func TestUnknownLoopIsAdoptedAndScheduledForward(t *testing.T) {
	l := newTestLoop(t, Loop{Name: "sweep", Message: "sweep", Every: "1m"})

	s, logs := testScheduler(t)
	now := time.Now()
	runPass(s, now)

	if _, ok := pendingTick(t, l.Name); ok {
		t.Fatal("adoption fired the loop instead of scheduling it")
	}
	st := armed(t, l.Name)
	if d := st.NextRunAt.Sub(now); d < 55*time.Second || d > 65*time.Second {
		t.Fatalf("adopted loop fires in %s, want ~1m", d)
	}
	if !strings.Contains(logs.String(), "adopted") {
		t.Fatalf("adoption was not logged:\n%s", logs.String())
	}
}

// A command that outlives its own interval must not have a second copy started
// on top of it — systemd refuses to restart a oneshot that is still running,
// and so do we.
func TestOverrunningLoopIsNotStartedTwice(t *testing.T) {
	l := newTestLoop(t, Loop{Name: "slow", Message: "slow", Every: "10s"})
	arm(t, l.Name, time.Now().Add(-time.Second))

	release := make(chan struct{})
	var mu sync.Mutex
	starts := 0
	s, logs := testScheduler(t)
	s.fire = func(name string) error {
		mu.Lock()
		starts++
		mu.Unlock()
		<-release
		return nil
	}

	s.pass(time.Now()) // starts the first fire, which blocks
	s.pass(time.Now()) // still due (state not advanced yet) but already running
	s.pass(time.Now())
	close(release)
	s.wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	if starts != 1 {
		t.Fatalf("started %d overlapping runs, want 1", starts)
	}
	if !strings.Contains(logs.String(), "still running") {
		t.Fatalf("the overrun was not reported:\n%s", logs.String())
	}
	if n := strings.Count(logs.String(), "still running"); n != 1 {
		t.Fatalf("the overrun was reported %d times; once per fire is enough", n)
	}
}

// A calendar loop schedules from the expression, not from a fixed interval, and
// the daemon must handle it without asking systemd anything.
func TestCalendarLoopFiresAndReschedulesFromTheExpression(t *testing.T) {
	l := newTestLoop(t, Loop{Name: "standup", Message: "draft the standup", Calendar: "Mon..Fri 09:00"})
	arm(t, l.Name, time.Now().Add(-time.Minute))

	s, _ := testScheduler(t)
	now := time.Now()
	runPass(s, now)

	if _, ok := pendingTick(t, l.Name); !ok {
		t.Fatal("the calendar loop did not fire")
	}
	st := armed(t, l.Name)
	want, err := nextCalendar(l.Calendar, now)
	if err != nil {
		t.Fatal(err)
	}
	if st.NextRunAt.Sub(want).Abs() > time.Minute {
		t.Fatalf("rescheduled to %s, want the next occurrence %s", st.NextRunAt, want)
	}
}

// A loop file hand-edited into something unschedulable must back off rather
// than spin: leaving its next-fire time in the past re-fires it every scan.
func TestUnschedulableLoopBacksOffInsteadOfSpinning(t *testing.T) {
	l := newTestLoop(t, Loop{Name: "broken", Message: "x", Every: "1m"})
	arm(t, l.Name, time.Now().Add(-time.Second))
	l.Every = "sometimes"
	if err := saveLoop(l); err != nil {
		t.Fatal(err)
	}

	s, logs := testScheduler(t)
	runPass(s, time.Now())

	st := armed(t, l.Name)
	if !st.NextRunAt.After(time.Now().Add(30 * time.Second)) {
		t.Fatalf("next fire %s does not back off; the loop will spin", st.NextRunAt)
	}
	if !strings.Contains(logs.String(), "cannot compute the next fire") {
		t.Fatalf("the bad schedule was not reported:\n%s", logs.String())
	}
}
