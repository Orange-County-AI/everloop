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

func createLoop(name, message, every, calendar string, enabled bool) (*Loop, error) {
	if err := validName(name); err != nil {
		return nil, err
	}
	if message == "" {
		return nil, fmt.Errorf("message is required")
	}
	if (every == "") == (calendar == "") {
		return nil, fmt.Errorf("exactly one of every/calendar is required")
	}
	if _, err := os.Stat(loopPath(name)); err == nil {
		return nil, fmt.Errorf("loop %q already exists", name)
	}
	if every != "" {
		if _, err := parseEvery(every); err != nil {
			return nil, err
		}
	}
	if calendar != "" {
		if err := validateCalendar(calendar); err != nil {
			return nil, err
		}
	}
	now := time.Now().UTC()
	l := &Loop{Name: name, Message: message, Every: every, Calendar: calendar, Enabled: enabled, CreatedAt: now, UpdatedAt: now}
	if err := saveLoop(l); err != nil {
		return nil, err
	}
	if err := installUnits(l); err != nil {
		os.Remove(loopPath(name))
		return nil, err
	}
	return l, nil
}

func updateLoop(name string, message, every, calendar *string, enabled *bool) (*Loop, error) {
	l, err := loadLoop(name)
	if err != nil {
		return nil, fmt.Errorf("loop %q not found", name)
	}
	if message != nil {
		if *message == "" {
			return nil, fmt.Errorf("message cannot be empty")
		}
		l.Message = *message
	}
	if every != nil && *every != "" {
		if _, err := parseEvery(*every); err != nil {
			return nil, err
		}
		l.Every, l.Calendar = *every, ""
	}
	if calendar != nil && *calendar != "" {
		if err := validateCalendar(*calendar); err != nil {
			return nil, err
		}
		l.Calendar, l.Every = *calendar, ""
	}
	if enabled != nil {
		l.Enabled = *enabled
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
	return os.Remove(loopPath(name))
}
