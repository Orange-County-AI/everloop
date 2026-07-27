package main

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

// Schedule parsing and the high-level loop operations shared by every
// platform backend. Each backend (systemd_linux.go, launchd_darwin.go)
// provides the same free functions:
//
//	installUnits(l *Loop) error    install/refresh the OS timer for a loop
//	removeUnits(name string) error tear down the OS timer for a loop
//	timerStatus(name string) string  human-readable live timer state
//	validateCalendar(expr string) error  check an OnCalendar expression

var durRe = regexp.MustCompile(`^(\d+d)?(\d+h)?(\d+m)?(\d+s)?$`)

// parseEvery accepts Go-style durations plus a "d" (days) unit: "90s", "5m",
// "1h30m", "2d12h".
func parseEvery(s string) (time.Duration, error) {
	if s == "" || !durRe.MatchString(s) {
		return 0, fmt.Errorf("invalid interval %q: use forms like 90s, 5m, 1h30m, 2d", s)
	}
	total := time.Duration(0)
	rest := s
	if i := strings.Index(rest, "d"); i > 0 {
		var days int
		if _, err := fmt.Sscanf(rest[:i+1], "%dd", &days); err != nil {
			return 0, fmt.Errorf("invalid interval %q", s)
		}
		total += time.Duration(days) * 24 * time.Hour
		rest = rest[i+1:]
	}
	if rest != "" {
		d, err := time.ParseDuration(rest)
		if err != nil {
			return 0, fmt.Errorf("invalid interval %q: %v", s, err)
		}
		total += d
	}
	if total < 10*time.Second {
		return 0, fmt.Errorf("interval %q too short: minimum 10s", s)
	}
	return total, nil
}

// --- high-level loop operations (shared by CLI and MCP tools) ----------------

// loopSpec is the mutable surface of a Loop, shared by create and update so
// both paths validate identically. A nil field means "leave alone"; the CLI
// only fills in flags the user actually passed (see setFlags) and the MCP tools
// get the same distinction free from JSON pointers.
type loopSpec struct {
	Message  *string
	Command  *string
	Timeout  *string
	Every    *string
	Calendar *string
	Enabled  *bool
}

// apply folds a spec into a loop and validates the result. Clearing semantics
// differ by field on purpose: Command and Timeout accept "" as an explicit
// clear (turning a watch back into a heartbeat), while Every/Calendar treat ""
// as "not supplied" — you switch schedule kinds by setting the other one, and
// a loop with neither has no way to fire.
func (s loopSpec) apply(l *Loop) error {
	if s.Message != nil {
		l.Message = *s.Message
	}
	if s.Command != nil {
		l.Command = *s.Command
	}
	if s.Timeout != nil {
		l.Timeout = *s.Timeout
	}
	if s.Every != nil && *s.Every != "" {
		l.Every, l.Calendar = *s.Every, ""
	}
	if s.Calendar != nil && *s.Calendar != "" {
		l.Calendar, l.Every = *s.Calendar, ""
	}
	if s.Enabled != nil {
		l.Enabled = *s.Enabled
	}

	if l.Command == "" {
		// A timeout is meaningless without a command; drop it silently when the
		// command is cleared so the leftover can't fail a later validation.
		l.Timeout = ""
		if l.Message == "" {
			return fmt.Errorf("message is required (or set a command)")
		}
	}
	if (l.Every == "") == (l.Calendar == "") {
		return fmt.Errorf("exactly one of every/calendar is required")
	}
	if l.Every != "" {
		if _, err := parseEvery(l.Every); err != nil {
			return err
		}
	}
	if l.Calendar != "" {
		if err := validateCalendar(l.Calendar); err != nil {
			return err
		}
	}
	if _, err := parseTimeout(l.Timeout); err != nil {
		return err
	}
	return nil
}

func createLoop(name string, spec loopSpec) (*Loop, error) {
	if err := validName(name); err != nil {
		return nil, err
	}
	if _, err := os.Stat(loopPath(name)); err == nil {
		return nil, fmt.Errorf("loop %q already exists", name)
	}
	now := time.Now().UTC()
	l := &Loop{Name: name, Enabled: true, CreatedAt: now, UpdatedAt: now}
	if err := spec.apply(l); err != nil {
		return nil, err
	}
	if err := saveLoop(l); err != nil {
		return nil, err
	}
	if err := installUnits(l); err != nil {
		os.Remove(loopPath(name))
		return nil, err
	}
	return l, nil
}

func updateLoop(name string, spec loopSpec) (*Loop, error) {
	l, err := loadLoop(name)
	if err != nil {
		return nil, fmt.Errorf("loop %q not found", name)
	}
	if err := spec.apply(l); err != nil {
		return nil, err
	}
	l.UpdatedAt = time.Now().UTC()
	if err := saveLoop(l); err != nil {
		return nil, err
	}
	if err := installUnits(l); err != nil {
		return nil, err
	}
	return l, nil
}

func deleteLoop(name string) error {
	if _, err := loadLoop(name); err != nil {
		return fmt.Errorf("loop %q not found", name)
	}
	if err := removeUnits(name); err != nil {
		return err
	}
	os.Remove(filepath.Join(queueDir(), "tick-"+name+".json"))
	os.Remove(runStatePath(name)) // damping memory dies with the loop
	return os.Remove(loopPath(name))
}
