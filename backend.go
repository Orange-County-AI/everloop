package main

import (
	"fmt"
	"os"
	"strings"
	"sync"
)

// Backend selection.
//
// schedule.go's contract — installUnits / removeUnits / timerStatus /
// validateCalendar — used to be satisfied by whichever build-tagged file
// compiled in. That no longer works, because the answer is not fixed at build
// time: the same Linux binary runs on a box with a working systemd user manager
// AND inside a container that has no systemd at all (our agent workspace pods
// mount /sys/fs/cgroup read-only and ship no systemctl, deliberately — giving
// them a writable cgroup tree would hand back the privileges the sandbox exists
// to remove). So the four functions dispatch at runtime to one of:
//
//	systemd   (linux)   ~/.config/systemd/user/*.timer      — systemd_linux.go
//	launchd   (darwin)  ~/Library/LaunchAgents/*.plist      — launchd_darwin.go
//	portable  (both)    everloop's own scheduler daemon     — portable.go
//
// EVERLOOP_BACKEND=auto|<native>|portable forces the choice. auto (the default)
// prefers the native OS scheduler when it is genuinely usable — probing, not
// assuming, because "runs on Linux" and "has a systemd user manager" are
// different facts — and falls back to portable otherwise.
//
// Every backend names itself in timerStatus, so `everloop list` and the
// list_loops tool always say which scheduler is actually holding a loop. That
// is not decoration: "my loop never fired" is the bug this file exists to
// prevent, and the first question it raises is which scheduler was supposed to
// fire it.
type backend interface {
	name() string
	installUnits(l *Loop) error
	removeUnits(name string) error
	timerStatus(name string) string
	validateCalendar(expr string) error
}

// Each platform file supplies its native backend and a probe for whether that
// backend can actually be driven here:
//
//	nativeBackend() backend
//	nativeUnusable() string   // "" when usable, else the human-readable reason

// resolveBackend maps EVERLOOP_BACKEND onto a backend. It always returns a
// usable backend, even alongside an error: a mis-set env should leave everloop
// working and complaining, not nil-dereferencing. main reports the error and
// exits before anything else runs.
func resolveBackend() (backend, error) {
	native := nativeBackend()
	switch v := strings.ToLower(strings.TrimSpace(os.Getenv("EVERLOOP_BACKEND"))); v {
	case "", "auto":
		if nativeUnusable() != "" {
			return portable{}, nil
		}
		return native, nil
	case "portable":
		return portable{}, nil
	case native.name():
		if reason := nativeUnusable(); reason != "" {
			return portable{}, fmt.Errorf("EVERLOOP_BACKEND=%s, but %s is not usable here: %s", v, v, reason)
		}
		return native, nil
	default:
		return portable{}, fmt.Errorf("unknown EVERLOOP_BACKEND %q: want auto, %s or portable", v, native.name())
	}
}

// activeBackend is resolved once per process. Probing systemd costs a subprocess
// and the answer cannot change under a running everloop.
var activeBackend = sync.OnceValue(func() backend {
	b, _ := resolveBackend()
	return b
})

// checkBackend surfaces a bad EVERLOOP_BACKEND at startup, next to
// checkInstance, so it fails on the command the operator just typed rather than
// silently scheduling somewhere they did not ask for. Only an explicit setting
// is checked: resolving auto costs a subprocess probe, and the hottest paths
// (`tick`, `send`) never touch a backend at all.
func checkBackend() error {
	if os.Getenv("EVERLOOP_BACKEND") == "" {
		return nil
	}
	_, err := resolveBackend()
	return err
}

func backendName() string { return activeBackend().name() }

func installUnits(l *Loop) error         { return activeBackend().installUnits(l) }
func removeUnits(name string) error      { return activeBackend().removeUnits(name) }
func timerStatus(name string) string     { return activeBackend().timerStatus(name) }
func validateCalendar(expr string) error { return activeBackend().validateCalendar(expr) }
