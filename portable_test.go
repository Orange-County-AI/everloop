package main

// Portable-backend tests: arming, disarming, the status string an operator
// reads when a loop "never fired", and the runtime backend choice.
//
// Every test drives portable{} directly rather than the free functions in
// backend.go. That is not incidental: the free functions dispatch to whatever
// this machine really has, so a test that used them would install live systemd
// units on a developer's box.

import (
	"os"
	"strings"
	"testing"
	"time"
)

// armed reads a loop's persisted schedule, failing the test if it has none.
func armed(t *testing.T, name string) schedState {
	t.Helper()
	st, ok := loadSchedState(name)
	if !ok {
		t.Fatalf("loop %q has no schedule state", name)
	}
	return st
}

func TestInstallArmsFromNowAndRemoveDisarms(t *testing.T) {
	l := newTestLoop(t, Loop{Name: "sweep", Message: "sweep", Every: "30m"})
	before := time.Now()
	if err := (portable{}).installUnits(l); err != nil {
		t.Fatal(err)
	}
	st := armed(t, l.Name)
	// Rearming from now mirrors systemd, where `systemctl restart` on the timer
	// resets OnActiveSec.
	if d := st.NextRunAt.Sub(before); d < 29*time.Minute || d > 31*time.Minute {
		t.Fatalf("next fire is %s away, want ~30m", d)
	}
	if err := (portable{}).removeUnits(l.Name); err != nil {
		t.Fatal(err)
	}
	if _, ok := loadSchedState(l.Name); ok {
		t.Fatal("schedule state survived removeUnits")
	}
	// Removing a loop that was never armed is not an error: delete must work
	// after a partial create.
	if err := (portable{}).removeUnits("never-existed"); err != nil {
		t.Fatal(err)
	}
}

func TestInstallRejectsAnUnusableSchedule(t *testing.T) {
	l := newTestLoop(t, Loop{Name: "bad", Message: "x", Calendar: "every other tuesday"})
	if err := (portable{}).installUnits(l); err == nil {
		t.Fatal("armed a loop whose calendar cannot be parsed")
	}
}

// The status line is the only place the "nobody is running the scheduler"
// failure is visible: the loop exists, is enabled, is armed, and will never
// fire. It has to shout.
func TestTimerStatusNamesTheBackendAndWarnsWithNoScheduler(t *testing.T) {
	l := newTestLoop(t, Loop{Name: "sweep", Message: "sweep", Every: "30m"})
	if err := (portable{}).installUnits(l); err != nil {
		t.Fatal(err)
	}
	got := (portable{}).timerStatus(l.Name)
	if !strings.HasPrefix(got, "portable: ") {
		t.Fatalf("status %q does not name the backend", got)
	}
	if !strings.Contains(got, "NO SCHEDULER RUNNING") {
		t.Fatalf("status %q does not warn that nothing is scheduling", got)
	}

	// With a scheduler holding the lock, the warning goes away and the next
	// fire is what is left.
	lock, err := lockScheduler()
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Close()
	if got := (portable{}).timerStatus(l.Name); strings.Contains(got, "NO SCHEDULER") {
		t.Fatalf("status %q still warns while a scheduler holds the lock", got)
	}

	l.Enabled = false
	if err := saveLoop(l); err != nil {
		t.Fatal(err)
	}
	if got := (portable{}).timerStatus(l.Name); got != "portable: disabled" {
		t.Fatalf("disabled loop status = %q", got)
	}
}

// A second daemon would double-fire every loop, so starting one must fail
// rather than race.
func TestSchedulerLockRefusesASecondInstance(t *testing.T) {
	t.Setenv("EVERLOOP_DATA_DIR", t.TempDir())
	if err := ensureDirs(); err != nil {
		t.Fatal(err)
	}
	if _, running := schedulerRunning(); running {
		t.Fatal("reported a running scheduler before one started")
	}
	first, err := lockScheduler()
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	if _, running := schedulerRunning(); !running {
		t.Fatal("did not report the running scheduler")
	}
	if _, err := lockScheduler(); err == nil {
		t.Fatal("a second scheduler took the lock")
	}
	first.Close()
	if _, running := schedulerRunning(); running {
		t.Fatal("still reports a scheduler after the lock was released")
	}
}

// Interval arithmetic: fires anchor to the SCHEDULED time so they do not drift
// by however long the command took, but an anchor already in the past snaps
// forward instead of scheduling into it — that snap is what makes a missed
// window one catch-up fire rather than a storm.
func TestNextFireAnchorsToScheduleAndSnapsForward(t *testing.T) {
	l := &Loop{Every: "10m"}
	scheduled := time.Now()

	next, err := nextFire(l, scheduled, scheduled.Add(30*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if !next.Equal(scheduled.Add(10 * time.Minute)) {
		t.Fatalf("next = %s, want the scheduled time + 10m (no drift)", next)
	}

	// Two hours late: twelve intervals missed, one fire scheduled.
	now := scheduled.Add(2 * time.Hour)
	next, err = nextFire(l, scheduled, now)
	if err != nil {
		t.Fatal(err)
	}
	if !next.Equal(now.Add(10 * time.Minute)) {
		t.Fatalf("next = %s, want now + 10m", next)
	}
	if got := missedFires(l, scheduled, now); got != 13 {
		t.Fatalf("missedFires = %d, want 13", got)
	}
	if got := missedFires(l, scheduled, scheduled.Add(-time.Minute)); got != 0 {
		t.Fatalf("missedFires before the due time = %d, want 0", got)
	}
}

// A calendar loop has no interval to divide by, so the miss count is walked
// from the expression. It is log-only, but a wrong number in the one line an
// operator reads after an outage is worse than no number.
func TestMissedFiresCountsCalendarOccurrences(t *testing.T) {
	l := &Loop{Calendar: "daily"}
	midnight := time.Now().In(time.Local).Truncate(24 * time.Hour)
	due := midnight.AddDate(0, 0, -3)

	if got := missedFires(l, due, midnight.Add(12*time.Hour)); got != 4 {
		t.Fatalf("missedFires over three days of downtime = %d, want 4", got)
	}
	if got := missedFires(l, midnight.AddDate(0, 0, 1), midnight.Add(time.Hour)); got != 0 {
		t.Fatalf("missedFires for a loop that is not due = %d, want 0", got)
	}
}

func TestResolveBackendHonoursTheEnvironment(t *testing.T) {
	t.Setenv("EVERLOOP_BACKEND", "portable")
	b, err := resolveBackend()
	if err != nil || b.name() != "portable" {
		t.Fatalf("forced portable = %q, %v", b.name(), err)
	}

	t.Setenv("EVERLOOP_BACKEND", "sysv")
	b, err = resolveBackend()
	if err == nil {
		t.Fatal("accepted an unknown backend")
	}
	// A bad setting still has to yield something that works: everloop should
	// complain, not panic.
	if b == nil || b.name() != "portable" {
		t.Fatalf("unknown backend did not fall back to portable: %v", b)
	}

	// Forcing the native backend must never downgrade silently: either you get
	// it, or you get told why not. Which of the two depends on the machine
	// running the tests, and that is the point — the invariant is that a
	// fallback is always accompanied by an error.
	t.Setenv("EVERLOOP_BACKEND", nativeBackend().name())
	b, err = resolveBackend()
	switch {
	case err == nil && b.name() != nativeBackend().name():
		t.Fatalf("forcing %s silently fell back to %s", nativeBackend().name(), b.name())
	case err != nil && !strings.Contains(err.Error(), "not usable here"):
		t.Fatalf("unusable native backend reported as %v, without saying so", err)
	}
}

// Fire history is the only record of when a loop last ran, so its write must
// survive the box losing power mid-rename: contents fsynced, then the directory
// entry.
func TestScheduleStateSurvivesReload(t *testing.T) {
	t.Setenv("EVERLOOP_DATA_DIR", t.TempDir())
	now := time.Now().UTC().Truncate(time.Second)
	if err := saveSchedState(schedState{Name: "sweep", NextRunAt: now, Fires: 3}); err != nil {
		t.Fatal(err)
	}
	st, ok := loadSchedState("sweep")
	if !ok || !st.NextRunAt.Equal(now) || st.Fires != 3 {
		t.Fatalf("reloaded state = %+v, ok=%v", st, ok)
	}
	if entries, _ := os.ReadDir(scheduleDir()); len(entries) != 1 {
		t.Fatalf("schedule dir holds %d files, want 1 (a .tmp was left behind)", len(entries))
	}

	// Corrupt state reads as absent so the daemon re-arms the loop, rather than
	// refusing to schedule it at all.
	if err := os.WriteFile(schedulePath("sweep"), []byte("{not json"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, ok := loadSchedState("sweep"); ok {
		t.Fatal("corrupt state reported as usable")
	}
}
