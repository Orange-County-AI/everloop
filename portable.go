package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// The portable backend: no OS timer manager at all, just state on disk plus
// `everloop scheduler` (scheduler.go) acting on it.
//
// The motivating host is an agent workspace pod — a stateful Linux container
// with sshd and sudo but no systemd, because /sys/fs/cgroup is mounted
// read-only and making it writable would hand back the privileges the sandbox
// exists to remove. Kubernetes CronJobs are not the answer either: loops are
// created at runtime by the agent, and the pods run with
// automountServiceAccountToken: false, so there is no API to create them with.
//
// systemd persists two things everloop never had to: when a loop fires next,
// and whether a fire was missed while the machine was down (Persistent=true).
// With no systemd there is nobody to ask, so this backend keeps that state
// itself, in schedule/<loop>.json beside the loop defs and the spool.
//
// The install and the firing happen in DIFFERENT processes — installUnits runs
// inside an MCP `serve` or a shell, the firing inside the long-lived daemon —
// so the state dir is the whole interface between them. There is no reload RPC
// and nothing to signal: the daemon re-reads the store every pass and treats it
// as the source of truth.
type portable struct{}

func (portable) name() string { return "portable" }

func scheduleDir() string             { return filepath.Join(dataDir(), "schedule") }
func schedulePath(name string) string { return filepath.Join(scheduleDir(), name+".json") }

// schedState is a loop's fire history — the part systemd would keep for us.
//
// NextRunAt is the entire contract with the daemon: the loop is due whenever it
// is in the past, whatever the reason (the interval elapsed, the container was
// down, the daemon was restarted mid-window). Nothing else needs to agree on
// why, which is what keeps catch-up from being a special case.
type schedState struct {
	Name      string    `json:"name"`
	NextRunAt time.Time `json:"next_run_at"`
	LastRunAt time.Time `json:"last_run_at,omitempty"`
	Fires     int       `json:"fires,omitempty"`
	CatchUps  int       `json:"catch_ups,omitempty"`
}

func loadSchedState(name string) (schedState, bool) {
	data, err := os.ReadFile(schedulePath(name))
	if err != nil {
		return schedState{Name: name}, false
	}
	var st schedState
	if err := json.Unmarshal(data, &st); err != nil {
		// Corrupt state reads as absent: the daemon re-arms the loop from now,
		// which costs at most one delayed fire. Refusing to schedule it at all
		// would be the silent stop everloop exists to prevent.
		return schedState{Name: name}, false
	}
	st.Name = name
	return st, true
}

// saveSchedState writes fire history durably. writeJSON's atomic rename is
// enough for the spool — a lost tick is a lost tick — but this file is the only
// record of when a loop last fired. If the box loses power with the rename in
// page cache, the loop comes back believing in a next-fire time that was never
// written, and either double-fires or (worse) sits idle past its window.
func saveSchedState(st schedState) error {
	if err := os.MkdirAll(scheduleDir(), 0o755); err != nil {
		return err
	}
	return writeJSONSync(schedulePath(st.Name), &st)
}

// writeJSONSync is writeJSON plus the two fsyncs that make the rename durable:
// the file's own contents, then the directory entry the rename created.
func writeJSONSync(path string, v any) error {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

// nextFire computes when a loop fires after `from`, with the clock at `now`.
//
// The anchor matters for intervals: the next fire is one interval after the
// SCHEDULED one, not one interval after whenever the daemon got round to it, or
// a 10m loop with a 40s command drifts by an hour a day. But an anchor in the
// past must never produce a fire in the past — that is how you turn one missed
// window into a firing storm — so the result is snapped forward to now+interval
// whenever the arithmetic lands behind. That snap IS the catch-up rule: a loop
// down for a day fires once and then resumes, exactly like systemd's
// Persistent=true, rather than once per missed interval.
func nextFire(l *Loop, from, now time.Time) (time.Time, error) {
	if l.Calendar != "" {
		return nextCalendar(l.Calendar, now)
	}
	d, err := parseEvery(l.Every)
	if err != nil {
		return time.Time{}, err
	}
	t := from.Add(d)
	if !t.After(now) {
		t = now.Add(d)
	}
	return t, nil
}

// missedFires is how many fires were skipped while nothing was scheduling —
// for the LOG only. The loop fires exactly once regardless, which is both
// systemd's behaviour and what the session is told to expect: a coalesced tick
// says "catch up once, do not repeat the work N times".
func missedFires(l *Loop, next, now time.Time) int {
	if !now.After(next) {
		return 0
	}
	if l.Calendar != "" {
		// Counting calendar occurrences means walking them; the daemon only
		// wants to say "you missed some", so don't pay for the exact number.
		return 1
	}
	d, err := parseEvery(l.Every)
	if err != nil || d <= 0 {
		return 1
	}
	return 1 + int(now.Sub(next)/d)
}

// installUnits arms a loop by writing when it fires next. There is nothing to
// start and nothing to reload: the daemon picks the change up on its next pass.
//
// Re-arming from now on every install mirrors systemd, where `systemctl restart`
// on the timer resets OnActiveSec — an update reschedules from now there too.
func (portable) installUnits(l *Loop) error {
	now := time.Now()
	next, err := nextFire(l, now, now)
	if err != nil {
		return err
	}
	st, _ := loadSchedState(l.Name)
	st.Name, st.NextRunAt = l.Name, next
	return saveSchedState(st)
}

func (portable) removeUnits(name string) error {
	if err := os.Remove(schedulePath(name)); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// timerStatus reports the next fire and, loudly, whether anything is running to
// deliver it. A loop can be perfectly scheduled and still never fire if nobody
// started the daemon; that failure is invisible everywhere else, so it is
// spelled out here — this string is what `everloop list` and the list_loops
// tool print.
func (portable) timerStatus(name string) string {
	warn := ""
	if _, running := schedulerRunning(); !running {
		warn = " — NO SCHEDULER RUNNING: start `everloop scheduler`"
	}
	l, err := loadLoop(name)
	if err != nil {
		return "portable: unknown"
	}
	if !l.Enabled {
		return "portable: disabled"
	}
	st, ok := loadSchedState(name)
	if !ok {
		return "portable: not armed (the scheduler will adopt it)" + warn
	}
	return fmt.Sprintf("portable: next %s%s", st.NextRunAt.Local().Format("Mon 2006-01-02 15:04:05 MST"), warn)
}

// validateCalendar rejects anything the portable scheduler cannot fire, at
// create time. An expression that parses but never occurs (*-02-30) is refused
// too: the loop would look installed and simply never run.
func (portable) validateCalendar(expr string) error {
	_, err := nextCalendar(expr, time.Now())
	return err
}
