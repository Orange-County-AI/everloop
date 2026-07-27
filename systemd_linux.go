//go:build linux

package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
)

// systemd backend: a .timer/.service pair per loop under
// ~/.config/systemd/user. The native backend on Linux — when there is a systemd
// user manager to talk to at all (see nativeUnusable).
type systemdBackend struct{}

func (systemdBackend) name() string { return "systemd" }

func nativeBackend() backend { return systemdBackend{} }

// nativeUnusable reports why systemd cannot schedule here, or "" when it can.
//
// Both failure modes are real and neither is exotic. Our workspace containers
// have no systemctl in the image at all; a box can also have the binary but no
// reachable user manager (no session bus, PID 1 is not systemd), where every
// systemctl call fails with "Failed to connect to bus". `show` is the cheapest
// call that proves the whole path works: it needs the binary, the environment
// and a live user manager on the other end.
var nativeUnusable = sync.OnceValue(func() string {
	if _, err := exec.LookPath("systemctl"); err != nil {
		return "systemctl not found in PATH"
	}
	if out, err := systemctl("show", "--property=Version"); err != nil {
		if line, _, ok := strings.Cut(strings.TrimSpace(out), "\n"); ok || line != "" {
			return strings.TrimSpace(line)
		}
		return err.Error()
	}
	return ""
})

func unitDir() string {
	home, _ := os.UserHomeDir()
	return filepath.Join(home, ".config", "systemd", "user")
}

// unitBase namespaces unit names by instance so two instances can each own a
// loop of the same name without colliding: everloop-<instance>-<loop>.
func unitBase() string {
	if inst := instanceName(); inst != "" {
		return "everloop-" + inst + "-"
	}
	return "everloop-"
}

func timerName(loop string) string   { return unitBase() + loop + ".timer" }
func serviceName(loop string) string { return unitBase() + loop + ".service" }

// instanceLabel is a human-readable "[instance] " prefix for unit descriptions.
func instanceLabel() string {
	if inst := instanceName(); inst != "" {
		return "[" + inst + "] "
	}
	return ""
}

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

func (systemdBackend) validateCalendar(expr string) error {
	cmd := exec.Command("systemd-analyze", "calendar", expr)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("invalid OnCalendar expression %q: %s", expr, strings.TrimSpace(string(out)))
	}
	return nil
}

// installUnits writes the .timer/.service pair for a loop and reloads systemd.
// The timer runs `everloop tick <name>`, which spools a message to the queue.
func (systemdBackend) installUnits(l *Loop) error {
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

	// Bake the isolation env into the unit so the timer-fired `tick` resolves
	// the same data dir this create used — without it, tick would read the
	// default instance, fail to find the loop, and spool nothing.
	var envLines string
	if inst := instanceName(); inst != "" {
		envLines += fmt.Sprintf("Environment=EVERLOOP_INSTANCE=%s\n", inst)
	}
	if d := os.Getenv("EVERLOOP_DATA_DIR"); d != "" {
		envLines += fmt.Sprintf("Environment=EVERLOOP_DATA_DIR=%s\n", d)
	}

	// A command loop must outlive systemd's default TimeoutStartSec (90s) long
	// enough to enforce its OWN timeout and spool the failure report. If systemd
	// killed the tick first, a hung command would fail exactly the way the
	// historical PATH bug did: invisibly.
	var svcLines string
	if l.Command != "" {
		d, err := parseTimeout(l.Timeout)
		if err != nil {
			return err
		}
		svcLines = fmt.Sprintf("TimeoutStartSec=%ds\n", int(d.Seconds())+30)
	}

	service := fmt.Sprintf(`[Unit]
Description=everloop tick: %s%s

[Service]
Type=oneshot
%s%sExecStart=%s tick %s
`, instanceLabel(), l.Name, envLines, svcLines, bin, l.Name)

	timer := fmt.Sprintf(`[Unit]
Description=everloop timer: %s%s

[Timer]
%s

[Install]
WantedBy=timers.target
`, instanceLabel(), l.Name, timerLines)

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

func (systemdBackend) removeUnits(name string) error {
	systemctl("disable", "--now", timerName(name))
	os.Remove(filepath.Join(unitDir(), timerName(name)))
	os.Remove(filepath.Join(unitDir(), serviceName(name)))
	_, err := systemctl("daemon-reload")
	return err
}

// timerStatus returns e.g. "systemd: active (next: Wed 2026-07-09 01:00:00)".
// The backend prefix is on every line so a list read in a bug report says which
// scheduler was meant to be holding the loop.
func (systemdBackend) timerStatus(name string) string {
	out, err := systemctl("show", timerName(name), "--property=ActiveState,NextElapseUSecRealtime")
	if err != nil {
		return "systemd: unknown"
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
		return fmt.Sprintf("systemd: %s (next: %s)", state, next)
	}
	return "systemd: " + state
}
