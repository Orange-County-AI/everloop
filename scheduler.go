package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

// `everloop scheduler` — the process that fires loops when the portable backend
// is live. It is what the systemd user manager is on a normal box, minus
// everything everloop does not need.
//
// Two properties shape the whole design.
//
// It is SUPERVISED, not self-supervising: it never daemonises, never forks,
// logs to stdout, and exits non-zero on anything it cannot recover from — so
// whatever starts it (in the workspace image, a small PID 1 next to sshd) can
// restart it and keep its logs. A single flock makes a second copy refuse to
// start rather than double-fire every loop.
//
// And it must NOT live inside `everloop serve`. That process is a child of the
// agent's Claude Code session, so loops scheduled there would die with the
// session — precisely the /loop and CronCreate failure mode everloop was built
// to fix, the one that left a merge-ready PR unreviewed for 44 hours. The
// daemon outlives sessions; `serve` only drains the spool.
//
// Because installs come from other processes entirely (an MCP serve, a shell),
// the daemon holds no authoritative in-memory schedule. Each pass re-reads the
// loop defs and the schedule state from disk. That single choice is also the
// reload mechanism, the catch-up mechanism and the crash-recovery mechanism:
// there is only ever one source of truth to consult.

const defaultScanInterval = time.Second

// scanInterval is both the clock granularity and the reload latency: a loop
// created in another process is picked up within one scan. 1s matches the
// AccuracySec=1s the systemd backend asks for.
func scanInterval() time.Duration {
	if s := os.Getenv("EVERLOOP_SCAN_SECONDS"); s != "" {
		if n, err := strconv.Atoi(s); err == nil && n >= 1 {
			return time.Duration(n) * time.Second
		}
	}
	return defaultScanInterval
}

// shutdownGrace bounds how long a stopping daemon waits for in-flight ticks. A
// command loop may legitimately be mid-run; anything still going after this
// re-fires on restart, which the spool's coalescing makes harmless.
const shutdownGrace = 10 * time.Second

func schedulerLockPath() string { return filepath.Join(dataDir(), "scheduler.lock") }
func schedulerPIDPath() string  { return filepath.Join(dataDir(), "scheduler.pid") }

// lockScheduler takes the single-instance lock. The lock is held for the life
// of the process (hence the returned file, which must not be closed early) and
// doubles as the liveness probe every other process uses — see schedulerRunning.
func lockScheduler() (*os.File, error) {
	f, err := os.OpenFile(schedulerLockPath(), os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		if pid, ok := schedulerRunning(); ok {
			return nil, fmt.Errorf("another everloop scheduler is already running (pid %d) for %s", pid, dataDir())
		}
		return nil, fmt.Errorf("cannot lock %s: %v", schedulerLockPath(), err)
	}
	return f, nil
}

// schedulerRunning reports whether a scheduler holds the lock, and the pid it
// last recorded. The lock is the authority — a pidfile alone goes stale the
// moment a container is killed, and a stale pidfile that claims a loop is being
// scheduled is exactly the lie this whole backend has to avoid telling.
func schedulerRunning() (int, bool) {
	f, err := os.OpenFile(schedulerLockPath(), os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return 0, false
	}
	defer f.Close()
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		pid := 0
		if data, err := os.ReadFile(schedulerPIDPath()); err == nil {
			pid, _ = strconv.Atoi(strings.TrimSpace(string(data)))
		}
		return pid, true
	}
	syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
	return 0, false
}

// warnIfNoScheduler is `serve`'s one job in all of this: say something when the
// portable backend is live and nothing is scheduling. Loops would be created
// happily, armed correctly, and never fire. It writes to stderr because stdout
// is the JSON-RPC channel.
func warnIfNoScheduler() {
	if backendName() != "portable" {
		return
	}
	if _, running := schedulerRunning(); !running {
		fmt.Fprintf(os.Stderr, "everloop: WARNING: portable backend is active but no `everloop scheduler` is running "+
			"for %s — loops will be created but will never fire.\n", dataDir())
	}
}

// --- the daemon ---------------------------------------------------------------

type scheduler struct {
	fire func(name string) error       // enqueueTick; swapped in tests
	logf func(format string, a ...any) // stdout; captured in tests
	now  func() time.Time

	mu       sync.Mutex
	inflight map[string]bool // loop -> already warned about the overrun
	unusable map[string]bool // loops whose schedule we have already complained about
	wg       sync.WaitGroup
}

func newScheduler() *scheduler {
	return &scheduler{
		fire:     enqueueTick,
		logf:     stdoutLogf,
		now:      time.Now,
		inflight: map[string]bool{},
		unusable: map[string]bool{},
	}
}

// stdoutLogf writes one line per event to stdout, unbuffered. No log file, no
// rotation, no levels: the supervisor captures stdout, and everything here is
// something an operator asked to happen or needs to know did not.
func stdoutLogf(format string, a ...any) {
	fmt.Printf("%s everloop[scheduler]: %s\n", time.Now().UTC().Format(time.RFC3339), fmt.Sprintf(format, a...))
}

// claim marks a loop as firing. A loop already in flight is skipped rather than
// started twice — systemd does the same, refusing to start a oneshot service
// that is still running — because a command loop whose command outlives its
// interval would otherwise fork a new copy every scan.
func (s *scheduler) claim(name string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if warned, running := s.inflight[name]; running {
		if !warned {
			s.inflight[name] = true
			s.logf("loop %q is still running from its last fire; skipping this one", name)
		}
		return false
	}
	s.inflight[name] = false
	return true
}

func (s *scheduler) release(name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.inflight, name)
}

// pass fires every loop that is due at `now`. It is the entire scheduler: read
// the store, compare against the persisted next-fire time, fire what is late.
// Startup catch-up needs no special case — a loop whose next fire is in the
// past is simply due, whether it went past a second ago or while the container
// was off for a day.
func (s *scheduler) pass(now time.Time) {
	loops, err := listLoops()
	if err != nil {
		s.logf("cannot read loops in %s: %v", loopsDir(), err)
		return
	}
	for _, l := range loops {
		if !l.Enabled {
			continue
		}
		st, armed := loadSchedState(l.Name)
		if !armed {
			s.adopt(l, now)
			continue
		}
		if st.NextRunAt.After(now) {
			continue
		}
		if !s.claim(l.Name) {
			continue
		}
		s.wg.Add(1)
		go s.runFire(l, st, now)
	}
}

// adopt arms a loop the daemon has never seen: one created while a different
// backend was active (a migration onto this host), or one whose state was lost.
// It schedules forward rather than firing now — on the first start after a
// migration every loop would otherwise fire at once, which looks exactly like
// the flood coalescing exists to prevent.
func (s *scheduler) adopt(l *Loop, now time.Time) {
	next, err := nextFire(l, now, now)
	if err != nil {
		// Reachable when a data dir moves from a systemd host, where the
		// calendar expression was systemd's to validate. Say it once: the pass
		// runs every second, and a line a second buries everything else in the
		// log the operator came to read.
		if !s.unusable[l.Name] {
			s.unusable[l.Name] = true
			s.logf("loop %q has an unusable schedule (%v); NOT scheduling it", l.Name, err)
		}
		return
	}
	st := schedState{Name: l.Name, NextRunAt: next}
	if err := saveSchedState(st); err != nil {
		s.logf("loop %q: cannot write schedule state: %v", l.Name, err)
		return
	}
	delete(s.unusable, l.Name)
	s.logf("adopted loop %q (%s): first fire %s", l.Name, scheduleDesc(l), next.Format(time.RFC3339))
}

// runFire performs one firing and advances the schedule.
func (s *scheduler) runFire(l *Loop, st schedState, now time.Time) {
	defer s.wg.Done()
	defer s.release(l.Name)

	// A missed window fires ONCE, not once per missed interval. That is
	// systemd's Persistent=true semantic, and it is the one the session is told
	// to expect: a coalesced tick says catch up once, do not repeat the work N
	// times. The count is logged because "it fired late" and "it fired late
	// because we were down for six hours" are different operational facts.
	if missed := missedFires(l, st.NextRunAt, now); missed > 1 {
		st.CatchUps++
		s.logf("catch-up: loop %q missed %d fires since %s — firing once",
			l.Name, missed, st.NextRunAt.Format(time.RFC3339))
	} else {
		s.logf("fire: loop %q (%s)", l.Name, scheduleDesc(l))
	}

	if err := s.fire(l.Name); err != nil {
		s.logf("loop %q tick failed: %v", l.Name, err)
	}

	// Advance AFTER the tick, never before. A crash in between re-fires the
	// loop once on restart — its next-fire time is still in the past — and the
	// spool coalesces that into the pending tick. Advancing first would turn
	// the same crash into a silently skipped fire, and a loop that quietly
	// stops is the failure everloop exists to make impossible. Delivery is
	// already at-least-once for exactly this reason.
	end := s.now()
	next, err := nextFire(l, st.NextRunAt, end)
	if err != nil {
		// Only reachable if the loop file was hand-edited into something
		// invalid. Back off instead of returning: leaving NextRunAt in the past
		// would re-fire this loop on every scan, forever.
		s.logf("loop %q: cannot compute the next fire (%v); retrying in a minute", l.Name, err)
		next = end.Add(time.Minute)
	}
	// An update landing mid-fire re-armed the loop from now, in another
	// process. That wins: it is a schedule the operator just chose, whereas
	// ours extends the one they replaced.
	if cur, ok := loadSchedState(l.Name); ok && !cur.NextRunAt.Equal(st.NextRunAt) {
		next = cur.NextRunAt
	}
	st.LastRunAt, st.Fires, st.NextRunAt = end.UTC(), st.Fires+1, next
	if err := saveSchedState(st); err != nil {
		s.logf("loop %q: cannot write schedule state (%v); it may fire again", l.Name, err)
	}
}

// drain waits for in-flight ticks, bounded. Anything still running past the
// grace period re-fires on restart rather than blocking the shutdown a
// supervisor is already timing.
func (s *scheduler) drain(grace time.Duration) {
	done := make(chan struct{})
	go func() { s.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(grace):
		s.logf("gave up waiting for in-flight ticks after %s; they re-fire on restart", grace)
	}
}

// runScheduler is the `everloop scheduler` entry point.
func runScheduler() error {
	if backendName() != "portable" {
		return fmt.Errorf("the %s backend is active and owns scheduling; `everloop scheduler` is only for the "+
			"portable backend (force it with EVERLOOP_BACKEND=portable)", backendName())
	}
	if err := ensureDirs(); err != nil {
		return err
	}
	lock, err := lockScheduler()
	if err != nil {
		return err
	}
	defer lock.Close()

	if err := os.WriteFile(schedulerPIDPath(), []byte(strconv.Itoa(os.Getpid())+"\n"), 0o644); err != nil {
		return err
	}
	defer os.Remove(schedulerPIDPath())

	// SIGTERM/SIGINT stop cleanly; SIGHUP just brings the next pass forward,
	// since every pass already re-reads the store. It exists so an operator who
	// created a loop by hand does not have to wait out the scan interval.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	hup := make(chan os.Signal, 1)
	signal.Notify(hup, syscall.SIGHUP)
	defer signal.Stop(hup)

	s := newScheduler()
	loops, _ := listLoops()
	s.logf("started: pid %d, instance %q, data %s, scan %s, %d loop(s)",
		os.Getpid(), instanceName(), dataDir(), scanInterval(), len(loops))

	ticker := time.NewTicker(scanInterval())
	defer ticker.Stop()
	for {
		s.pass(time.Now())
		select {
		case <-ctx.Done():
			s.logf("stopping; waiting up to %s for in-flight ticks", shutdownGrace)
			s.drain(shutdownGrace)
			s.logf("stopped")
			return nil
		case <-hup:
			s.logf("SIGHUP: rescanning now")
		case <-ticker.C:
		}
	}
}
