// everloop: persistent loops for a coding agent, backed by whichever scheduler
// the host actually has — systemd user timers, launchd agents, or everloop's own
// scheduler daemon where there is no init system to borrow (containers).
//
// Delivery reaches the agent as a Mattermost DM by default, or over herdr's
// unix socket with CHANNEL_SINK=herdr — so the agent on the other end can be
// claude, codex, omp, opencode or pi. See sink.go.
//
// One binary, four roles:
//   - `everloop serve`      the MCP server the agent spawns (stdio), which also
//     drains the spool into the configured sink
//   - `everloop scheduler`  the supervised daemon that fires loops when the
//     portable backend is live (see backend.go for the choice)
//   - `everloop tick NAME`  one firing, spooled: what a timer executes
//   - CLI loop management   create/list/update/delete/send, mirroring the MCP tools
package main

import (
	"flag"
	"fmt"
	"os"
)

const version = "0.1.0"

const usage = `everloop %s - persistent loops for a coding agent
(systemd user timers, launchd agents, or everloop's own scheduler daemon)

Usage:
  everloop serve                                 run as MCP server + spool drain (stdio)
  everloop scheduler                             run the loop scheduler (portable backend; supervise this)
  everloop create NAME (--message M | --command C) (--every D | --calendar C) [--timeout T] [--disabled]
  everloop list
  everloop update NAME [--message M] [--command C] [--timeout T] [--every D] [--calendar C] [--enable|--disable]
  everloop delete NAME
  everloop send MESSAGE                          spool an ad-hoc message to the session
  everloop tick NAME                             (called by the timer) spool one loop firing

Intervals: 90s, 5m, 1h30m, 2d (min 10s). Calendar: OnCalendar syntax such as
"daily", "Mon..Fri 09:00" or "*-*-* 09:00:00" (the launchd and portable backends
support a subset and reject the rest at create time).
With --command the loop is a watch: the command runs each firing and an event is
spooled only when it writes to stdout (--timeout bounds it, default 60s). The
command runs in the scheduler's environment, not a login shell.

Backend: EVERLOOP_BACKEND=auto|systemd|launchd|portable (default auto — the OS
timer manager when it is usable, else portable). "everloop list" names the live
backend per loop. The portable backend fires nothing unless "everloop scheduler"
is running.
State: ~/.local/share/everloop (loop defs, spool, schedule state)
Timers: ~/.config/systemd/user/everloop-*.timer (Linux)
        ~/Library/LaunchAgents/com.52labs.everloop.*.plist (macOS)
`

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, usage, version)
		os.Exit(2)
	}
	if err := checkInstance(); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
	// Resolve the backend before doing anything: a mis-set EVERLOOP_BACKEND
	// should fail on the command the operator just typed, not by scheduling
	// somewhere they did not ask for.
	if err := checkBackend(); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
	var err error
	switch os.Args[1] {
	case "serve":
		err = serve()
	case "scheduler":
		err = runScheduler()
	case "tick":
		if len(os.Args) != 3 {
			err = fmt.Errorf("usage: everloop tick NAME")
		} else {
			err = enqueueTick(os.Args[2])
		}
	case "create":
		err = cmdCreate(os.Args[2:])
	case "list":
		err = cmdList()
	case "update":
		err = cmdUpdate(os.Args[2:])
	case "delete":
		if len(os.Args) != 3 {
			err = fmt.Errorf("usage: everloop delete NAME")
		} else if err = deleteLoop(os.Args[2]); err == nil {
			fmt.Printf("Deleted loop %q.\n", os.Args[2])
		}
	case "send":
		if len(os.Args) != 3 {
			err = fmt.Errorf("usage: everloop send MESSAGE")
		} else if err = enqueueMessage(os.Args[2], nil); err == nil {
			fmt.Println("Message spooled.")
		}
	case "version", "--version", "-v":
		fmt.Printf("%s (backend: %s)\n", version, backendName())
	case "help", "--help", "-h":
		fmt.Printf(usage, version)
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n\n"+usage, os.Args[1], version)
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
