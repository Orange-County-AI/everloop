//go:build darwin

package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

// launchd backend: one LaunchAgent plist per loop under ~/Library/LaunchAgents,
// labelled com.52labs.everloop[.<instance>].<name>. Interval loops use
// StartInterval; calendar loops use StartCalendarInterval (a supported subset
// of systemd's OnCalendar syntax — see parseCalendar).
//
// The native backend on macOS. launchd is part of the OS, so unlike systemd on
// Linux it is never missing; the probe only guards against launchctl being
// unreachable (a stripped container image, a locked-down sandbox).
type launchdBackend struct{}

func (launchdBackend) name() string { return "launchd" }

func nativeBackend() backend { return launchdBackend{} }

var nativeUnusable = sync.OnceValue(func() string {
	if _, err := exec.LookPath("launchctl"); err != nil {
		return "launchctl not found in PATH"
	}
	if _, err := launchctl("print", guiDomain()); err != nil {
		return "no launchd GUI domain for uid " + strconv.Itoa(os.Getuid())
	}
	return ""
})

func agentsDir() string {
	home, _ := os.UserHomeDir()
	return filepath.Join(home, "Library", "LaunchAgents")
}

// loopLabel namespaces labels by instance so two instances can each own a
// loop of the same name without colliding.
func loopLabel(loop string) string {
	if inst := instanceName(); inst != "" {
		return "com.52labs.everloop." + inst + "." + loop
	}
	return "com.52labs.everloop." + loop
}

func plistPath(loop string) string {
	return filepath.Join(agentsDir(), loopLabel(loop)+".plist")
}

func logDir() string {
	home, _ := os.UserHomeDir()
	return filepath.Join(home, "Library", "Logs", "everloop")
}

func guiDomain() string { return fmt.Sprintf("gui/%d", os.Getuid()) }

func launchctl(args ...string) (string, error) {
	out, err := exec.Command("launchctl", args...).CombinedOutput()
	if err != nil {
		return string(out), fmt.Errorf("launchctl %s: %v: %s",
			strings.Join(args, " "), err, strings.TrimSpace(string(out)))
	}
	return string(out), nil
}

// --- OnCalendar subset -> StartCalendarInterval ------------------------------

var weekdays = map[string]int{
	"sun": 0, "mon": 1, "tue": 2, "wed": 3, "thu": 4, "fri": 5, "sat": 6,
}

// calEntry is one StartCalendarInterval key in a stable order.
type calEntry struct {
	key   string
	value int
}

// parseCalendar maps a supported subset of systemd OnCalendar syntax to a
// launchd StartCalendarInterval dict:
//
//	hourly                       -> {Minute: 0}
//	daily                        -> {Hour: 0, Minute: 0}
//	weekly                       -> {Weekday: 1, Hour: 0, Minute: 0}
//	*-*-* HH:MM[:SS]             -> {Hour, Minute}
//	<Weekday> *-*-* HH:MM[:SS]   -> {Weekday, Hour, Minute}
//
// launchd has no seconds field, so a :SS component is accepted but ignored.
func parseCalendar(expr string) ([]calEntry, error) {
	unsupported := fmt.Errorf("OnCalendar expression %q not supported on macOS: use hourly, daily, weekly, \"*-*-* HH:MM\", or \"Mon *-*-* HH:MM\"", expr)

	switch strings.ToLower(strings.TrimSpace(expr)) {
	case "hourly":
		return []calEntry{{"Minute", 0}}, nil
	case "daily":
		return []calEntry{{"Hour", 0}, {"Minute", 0}}, nil
	case "weekly": // systemd weekly = Mon 00:00
		return []calEntry{{"Weekday", 1}, {"Hour", 0}, {"Minute", 0}}, nil
	}

	fields := strings.Fields(expr)
	weekday := -1
	if len(fields) == 3 {
		wd, ok := weekdays[strings.ToLower(fields[0])]
		if !ok {
			return nil, unsupported
		}
		weekday, fields = wd, fields[1:]
	}
	if len(fields) != 2 || fields[0] != "*-*-*" {
		return nil, unsupported
	}
	parts := strings.Split(fields[1], ":")
	if len(parts) != 2 && len(parts) != 3 {
		return nil, unsupported
	}
	hour, err := strconv.Atoi(parts[0])
	if err != nil || hour < 0 || hour > 23 {
		return nil, unsupported
	}
	minute, err := strconv.Atoi(parts[1])
	if err != nil || minute < 0 || minute > 59 {
		return nil, unsupported
	}
	if len(parts) == 3 {
		if sec, err := strconv.Atoi(parts[2]); err != nil || sec < 0 || sec > 59 {
			return nil, unsupported
		}
	}
	entries := []calEntry{}
	if weekday >= 0 {
		entries = append(entries, calEntry{"Weekday", weekday})
	}
	return append(entries, calEntry{"Hour", hour}, calEntry{"Minute", minute}), nil
}

func (launchdBackend) validateCalendar(expr string) error {
	_, err := parseCalendar(expr)
	return err
}

// --- plist generation ---------------------------------------------------------

func xmlEscape(s string) string {
	r := strings.NewReplacer("&", "&amp;", "<", "&lt;", ">", "&gt;", `"`, "&quot;")
	return r.Replace(s)
}

// renderPlist builds the LaunchAgent plist for a loop.
func renderPlist(l *Loop, bin string) (string, error) {
	var b strings.Builder
	b.WriteString(`<?xml version="1.0" encoding="UTF-8"?>` + "\n")
	b.WriteString(`<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">` + "\n")
	b.WriteString("<plist version=\"1.0\">\n<dict>\n")

	kv := func(key, tag, val string) {
		fmt.Fprintf(&b, "\t<key>%s</key>\n\t<%s>%s</%s>\n", xmlEscape(key), tag, xmlEscape(val), tag)
	}
	kbool := func(key string, v bool) {
		fmt.Fprintf(&b, "\t<key>%s</key>\n\t<%v/>\n", xmlEscape(key), v)
	}

	kv("Label", "string", loopLabel(l.Name))

	b.WriteString("\t<key>ProgramArguments</key>\n\t<array>\n")
	for _, arg := range []string{bin, "tick", l.Name} {
		fmt.Fprintf(&b, "\t\t<string>%s</string>\n", xmlEscape(arg))
	}
	b.WriteString("\t</array>\n")

	if l.Calendar != "" {
		entries, err := parseCalendar(l.Calendar)
		if err != nil {
			return "", err
		}
		b.WriteString("\t<key>StartCalendarInterval</key>\n\t<dict>\n")
		for _, e := range entries {
			fmt.Fprintf(&b, "\t\t<key>%s</key>\n\t\t<integer>%d</integer>\n", e.key, e.value)
		}
		b.WriteString("\t</dict>\n")
	} else {
		d, err := parseEvery(l.Every)
		if err != nil {
			return "", err
		}
		kv("StartInterval", "integer", strconv.Itoa(int(d.Seconds())))
	}

	kbool("RunAtLoad", false)
	kv("ProcessType", "string", "Background")

	// Bake the isolation env into the plist so the timer-fired `tick` resolves
	// the same data dir this create used (mirrors the systemd Environment= lines).
	env := map[string]string{}
	if inst := instanceName(); inst != "" {
		env["EVERLOOP_INSTANCE"] = inst
	}
	if d := os.Getenv("EVERLOOP_DATA_DIR"); d != "" {
		env["EVERLOOP_DATA_DIR"] = d
	}
	if len(env) > 0 {
		b.WriteString("\t<key>EnvironmentVariables</key>\n\t<dict>\n")
		for _, k := range []string{"EVERLOOP_DATA_DIR", "EVERLOOP_INSTANCE"} {
			if v, ok := env[k]; ok {
				fmt.Fprintf(&b, "\t\t<key>%s</key>\n\t\t<string>%s</string>\n", k, xmlEscape(v))
			}
		}
		b.WriteString("\t</dict>\n")
	}

	logFile := filepath.Join(logDir(), l.Name+".log")
	kv("StandardOutPath", "string", logFile)
	kv("StandardErrorPath", "string", logFile)

	b.WriteString("</dict>\n</plist>\n")
	return b.String(), nil
}

// --- backend operations --------------------------------------------------------

// installUnits writes the LaunchAgent plist for a loop and (re)bootstraps it.
// Disabled loops keep their plist on disk but stay booted out; we never touch
// `launchctl disable`, whose override DB outlives the plist.
func (launchdBackend) installUnits(l *Loop) error {
	bin, err := os.Executable()
	if err != nil {
		return err
	}
	bin, _ = filepath.EvalSymlinks(bin)

	content, err := renderPlist(l, bin)
	if err != nil {
		return err
	}
	for _, d := range []string{agentsDir(), logDir()} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			return err
		}
	}
	path := plistPath(l.Name)
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		return err
	}
	// bootout first so bootstrap re-reads the plist (schedule changes apply);
	// it fails harmlessly when the job was not loaded.
	launchctl("bootout", guiDomain()+"/"+loopLabel(l.Name))
	if l.Enabled {
		if _, err := launchctl("bootstrap", guiDomain(), path); err != nil {
			return err
		}
	}
	return nil
}

func (launchdBackend) removeUnits(name string) error {
	launchctl("bootout", guiDomain()+"/"+loopLabel(name))
	os.Remove(plistPath(name))
	return nil
}

// timerStatus reports whether the LaunchAgent is loaded. launchd exposes no
// next-fire time, so this is coarser than the systemd equivalent.
func (launchdBackend) timerStatus(name string) string {
	out, err := launchctl("print", guiDomain()+"/"+loopLabel(name))
	if err != nil {
		return "launchd: not loaded"
	}
	for _, line := range strings.Split(out, "\n") {
		t := strings.TrimSpace(line)
		if v, ok := strings.CutPrefix(t, "state = "); ok {
			return "launchd: loaded (state = " + strings.TrimSpace(v) + ")"
		}
	}
	return "launchd: loaded"
}
