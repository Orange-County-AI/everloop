// everloop: persistent loops for Claude Code, backed by OS user timers
// (systemd on Linux, launchd on macOS).
//
// One binary, three roles:
//   - `everloop serve`      the MCP channel server Claude Code spawns (stdio)
//   - `everloop tick NAME`  what each OS timer executes: spool one firing
//   - CLI loop management   create/list/update/delete/send, mirroring the MCP tools
package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
)

const version = "0.1.0"

const usage = `everloop %s - persistent loops for Claude Code, backed by OS timers
(systemd user timers on Linux, launchd agents on macOS)

Usage:
  everloop serve                                 run as MCP channel server (stdio)
  everloop create NAME (--message M | --command C) (--every D | --calendar C) [--timeout T] [--disabled]
  everloop list
  everloop update NAME [--message M] [--command C] [--timeout T] [--every D] [--calendar C] [--enable|--disable]
  everloop delete NAME
  everloop send MESSAGE                          spool an ad-hoc message to the session
  everloop tick NAME                             (called by the OS timer) spool one loop firing

Global:
  --instance NAME    pick the instance to act on (its own data dir and timer
                     namespace). Valid on every subcommand and anywhere in the
                     argument list. Overrides EVERLOOP_INSTANCE; unset = the
                     default instance.

Intervals: 90s, 5m, 1h30m, 2d (min 10s). Calendar: systemd OnCalendar syntax
(macOS supports a subset: hourly, daily, weekly, "*-*-* HH:MM", "Mon *-*-* HH:MM").
With --command the loop is a watch: the command runs each firing and an event is
spooled only when it writes to stdout (--timeout bounds it, default 60s). The
command runs in the OS timer's environment, not a login shell.
State: ~/.local/share/everloop
Timers: ~/.config/systemd/user/everloop-*.timer (Linux)
        ~/Library/LaunchAgents/com.52labs.everloop.*.plist (macOS)
`

// splitInstanceFlag pulls the global --instance out of the argument list before
// any subcommand parser sees it, so it can appear anywhere: `everloop
// --instance jessica list` and `everloop list --instance jessica` are the same
// command, and `serve --instance jessica` lets an .mcp.json entry select an
// instance with args instead of an env block. Both `--instance NAME` and
// `--instance=NAME` are accepted (as is the single-dash spelling the flag
// package allows), and a bare `--` ends the scan so a literal argument after it
// is never eaten.
func splitInstanceFlag(args []string) ([]string, error) {
	rest := make([]string, 0, len(args))
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if arg == "--" {
			return append(rest, args[i:]...), nil
		}
		name, ok := strings.CutPrefix(arg, "--")
		if !ok {
			name, ok = strings.CutPrefix(arg, "-")
		}
		if !ok {
			rest = append(rest, arg)
			continue
		}
		name, value, hasValue := strings.Cut(name, "=")
		if name != "instance" {
			rest = append(rest, arg)
			continue
		}
		if !hasValue {
			if i+1 >= len(args) {
				return nil, fmt.Errorf("flag needs an argument: --instance")
			}
			i++
			value = args[i]
		}
		instanceFlag, instanceFlagSet = value, true
	}
	return rest, nil
}

func main() {
	args, err := splitInstanceFlag(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(2)
	}
	if len(args) == 0 {
		fmt.Fprintf(os.Stderr, usage, version)
		os.Exit(2)
	}
	if err := checkInstance(); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
	switch args[0] {
	case "serve":
		err = serve()
	case "tick":
		if len(args) != 2 {
			err = fmt.Errorf("usage: everloop tick NAME")
		} else {
			err = enqueueTick(args[1])
		}
	case "create":
		err = cmdCreate(args[1:])
	case "list":
		err = cmdList()
	case "update":
		err = cmdUpdate(args[1:])
	case "delete":
		if len(args) != 2 {
			err = fmt.Errorf("usage: everloop delete NAME")
		} else if err = deleteLoop(args[1]); err == nil {
			fmt.Printf("Deleted loop %q.\n", args[1])
		}
	case "send":
		if len(args) != 2 {
			err = fmt.Errorf("usage: everloop send MESSAGE")
		} else if err = enqueueMessage(args[1], nil); err == nil {
			fmt.Println("Message spooled.")
		}
	case "version", "--version", "-v":
		fmt.Println(version)
	case "help", "--help", "-h":
		fmt.Printf(usage, version)
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n\n"+usage, args[0], version)
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

// loopFlags registers the flags create and update share and returns a spec
// carrying only the ones the user actually passed. The distinction matters:
// `--command ""` must clear a command (turning a watch back into a heartbeat),
// while an omitted --command must leave it alone.
func loopFlags(fs *flag.FlagSet, args []string) (loopSpec, error) {
	message := fs.String("message", "", "instruction delivered on each firing")
	command := fs.String("command", "", "run this each firing; spool only if it writes to stdout")
	timeout := fs.String("timeout", "", "max command runtime (default 60s)")
	every := fs.String("every", "", "interval (90s, 5m, 1h30m, 2d)")
	calendar := fs.String("calendar", "", "systemd OnCalendar expression")
	if err := fs.Parse(args); err != nil {
		return loopSpec{}, err
	}
	seen := map[string]bool{}
	fs.Visit(func(f *flag.Flag) { seen[f.Name] = true })
	var spec loopSpec
	if seen["message"] {
		spec.Message = message
	}
	if seen["command"] {
		spec.Command = command
	}
	if seen["timeout"] {
		spec.Timeout = timeout
	}
	if seen["every"] {
		spec.Every = every
	}
	if seen["calendar"] {
		spec.Calendar = calendar
	}
	return spec, nil
}

func cmdCreate(args []string) error {
	if len(args) < 1 || args[0] == "" || args[0][0] == '-' {
		return fmt.Errorf("usage: everloop create NAME (--message M | --command C) (--every D | --calendar C)")
	}
	name := args[0]
	fs := flag.NewFlagSet("create", flag.ContinueOnError)
	disabled := fs.Bool("disabled", false, "create without starting the timer")
	spec, err := loopFlags(fs, args[1:])
	if err != nil {
		return err
	}
	if *disabled {
		f := false
		spec.Enabled = &f
	}
	l, err := createLoop(name, spec)
	if err != nil {
		return err
	}
	fmt.Printf("Created loop %q (%s). Timer: %s\n", l.Name, scheduleDesc(l), timerStatus(l.Name))
	return nil
}

func cmdList() error {
	loops, err := listLoops()
	if err != nil {
		return err
	}
	if len(loops) == 0 {
		fmt.Println("No loops defined.")
		return nil
	}
	for _, l := range loops {
		fmt.Printf("- %s: %s | enabled=%v | timer=%s\n", l.Name, scheduleDesc(l), l.Enabled, timerStatus(l.Name))
		if l.Command != "" {
			fmt.Printf("    command: %s (timeout %s)\n", l.Command, timeoutDesc(l))
		}
		if l.Message != "" {
			fmt.Printf("    message: %s\n", l.Message)
		}
	}
	return nil
}

func cmdUpdate(args []string) error {
	if len(args) < 1 || args[0] == "" || args[0][0] == '-' {
		return fmt.Errorf("usage: everloop update NAME [--message M] [--command C] [--timeout T] [--every D] [--calendar C] [--enable|--disable]")
	}
	name := args[0]
	fs := flag.NewFlagSet("update", flag.ContinueOnError)
	enable := fs.Bool("enable", false, "enable the timer")
	disable := fs.Bool("disable", false, "disable the timer")
	spec, err := loopFlags(fs, args[1:])
	if err != nil {
		return err
	}
	if *enable {
		t := true
		spec.Enabled = &t
	}
	if *disable {
		f := false
		spec.Enabled = &f
	}
	l, err := updateLoop(name, spec)
	if err != nil {
		return err
	}
	fmt.Printf("Updated loop %q (%s, enabled=%v). Timer: %s\n", l.Name, scheduleDesc(l), l.Enabled, timerStatus(l.Name))
	return nil
}
