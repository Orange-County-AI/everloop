package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

const unitPrefix = "everloop-"

func unitDir() string {
	home, _ := os.UserHomeDir()
	return filepath.Join(home, ".config", "systemd", "user")
}

func timerName(loop string) string   { return unitPrefix + loop + ".timer" }
func serviceName(loop string) string { return unitPrefix + loop + ".service" }

// systemctl runs `systemctl --user <args>`, ensuring XDG_RUNTIME_DIR is set
// even when spawned from an environment that lacks it (e.g. an MCP subprocess).
func systemctl(args ...string) (string, error) {
	cmd := exec.Command("systemctl", append([]string{"--user"}, args...)...)
	cmd.Env = os.Environ()
	if os.Getenv("XDG_RUNTIME_DIR") == "" {
		cmd.Env = append(cmd.Env, fmt.Sprintf("XDG_RUNTIME_DIR=/run/user/%d", os.Getuid()))
	}
	out, err := cmd.CombinedOutput()
	if err != nil {
		return string(out), fmt.Errorf("systemctl %s: %v: %s", strings.Join(args, " "), err, strings.TrimSpace(string(out)))
	}
	return string(out), nil
}

var durRe = regexp.MustCompile(`^(\d+d)?(\d+h)?(\d+m)?(\d+s)?$`)

// parseEvery accepts Go-style durations plus a "d" (days) unit: "90s", "5m",
// "1h30m", "2d12h". Returns a normalized systemd time span.
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

func validateCalendar(expr string) error {
	cmd := exec.Command("systemd-analyze", "calendar", expr)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("invalid OnCalendar expression %q: %s", expr, strings.TrimSpace(string(out)))
	}
	return nil
}

// installUnits writes the .timer/.service pair for a loop and reloads systemd.
// The timer runs `everloop tick <name>`, which spools a message to the queue.
func installUnits(l *Loop) error {
	bin, err := os.Executable()
	if err != nil {
		return err
	}
	bin, _ = filepath.EvalSymlinks(bin)

	var timerLines string
	if l.Calendar != "" {
		// Persistent=true: a missed calendar fire (machine off) runs on boot.
		timerLines = fmt.Sprintf("OnCalendar=%s\nPersistent=true", l.Calendar)
	} else {
		d, err := parseEvery(l.Every)
		if err != nil {
			return err
		}
		span := fmt.Sprintf("%ds", int(d.Seconds()))
		// OnActiveSec anchors the first fire; OnUnitActiveSec repeats after it.
		timerLines = fmt.Sprintf("OnActiveSec=%s\nOnUnitActiveSec=%s\nAccuracySec=1s", span, span)
	}

	service := fmt.Sprintf(`[Unit]
Description=everloop tick: %s

[Service]
Type=oneshot
ExecStart=%s tick %s
`, l.Name, bin, l.Name)

	timer := fmt.Sprintf(`[Unit]
Description=everloop timer: %s

[Timer]
%s

[Install]
WantedBy=timers.target
`, l.Name, timerLines)

	if err := os.MkdirAll(unitDir(), 0o755); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(unitDir(), serviceName(l.Name)), []byte(service), 0o644); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(unitDir(), timerName(l.Name)), []byte(timer), 0o644); err != nil {
		return err
	}
	if _, err := systemctl("daemon-reload"); err != nil {
		return err
	}
	if l.Enabled {
		if _, err := systemctl("enable", "--now", timerName(l.Name)); err != nil {
			return err
		}
		// restart applies interval changes to an already-active timer
		if _, err := systemctl("restart", timerName(l.Name)); err != nil {
			return err
		}
	} else {
		systemctl("disable", "--now", timerName(l.Name))
	}
	return nil
}

func removeUnits(name string) error {
	systemctl("disable", "--now", timerName(name))
	os.Remove(filepath.Join(unitDir(), timerName(name)))
	os.Remove(filepath.Join(unitDir(), serviceName(name)))
	_, err := systemctl("daemon-reload")
	return err
}

// timerStatus returns e.g. "active (next: Wed 2026-07-09 01:00:00)".
func timerStatus(name string) string {
	out, err := systemctl("show", timerName(name), "--property=ActiveState,NextElapseUSecRealtime")
	if err != nil {
		return "unknown"
	}
	state, next := "unknown", ""
	for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
		if v, ok := strings.CutPrefix(line, "ActiveState="); ok {
			state = v
		}
		if v, ok := strings.CutPrefix(line, "NextElapseUSecRealtime="); ok {
			next = v
		}
	}
	if next != "" && state == "active" {
		return fmt.Sprintf("%s (next: %s)", state, next)
	}
	return state
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
