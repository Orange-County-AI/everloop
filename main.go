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
)

const version = "0.1.0"

const usage = `everloop %s - persistent loops for Claude Code, backed by OS timers
(systemd user timers on Linux, launchd agents on macOS)

Usage:
  everloop serve                                 run as MCP channel server (stdio)
  everloop create NAME --message M (--every D | --calendar C) [--disabled]
  everloop list
  everloop update NAME [--message M] [--every D] [--calendar C] [--enable|--disable]
  everloop delete NAME
  everloop send MESSAGE                          spool an ad-hoc message to the session
  everloop tick NAME                             (called by the OS timer) spool one loop firing

Intervals: 90s, 5m, 1h30m, 2d (min 10s). Calendar: systemd OnCalendar syntax
(macOS supports a subset: hourly, daily, weekly, "*-*-* HH:MM", "Mon *-*-* HH:MM").
State: ~/.local/share/everloop
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
	var err error
	switch os.Args[1] {
	case "serve":
		err = serve()
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
		fmt.Println(version)
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

func cmdCreate(args []string) error {
	if len(args) < 1 || args[0] == "" || args[0][0] == '-' {
		return fmt.Errorf("usage: everloop create NAME --message M (--every D | --calendar C)")
	}
	name := args[0]
	fs := flag.NewFlagSet("create", flag.ContinueOnError)
	message := fs.String("message", "", "instruction delivered on each firing")
	every := fs.String("every", "", "interval (90s, 5m, 1h30m, 2d)")
	calendar := fs.String("calendar", "", "systemd OnCalendar expression")
	disabled := fs.Bool("disabled", false, "create without starting the timer")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	l, err := createLoop(name, *message, *every, *calendar, !*disabled)
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
		fmt.Printf("- %s: %s | enabled=%v | timer=%s\n    message: %s\n",
			l.Name, scheduleDesc(l), l.Enabled, timerStatus(l.Name), l.Message)
	}
	return nil
}

func cmdUpdate(args []string) error {
	if len(args) < 1 || args[0] == "" || args[0][0] == '-' {
		return fmt.Errorf("usage: everloop update NAME [--message M] [--every D] [--calendar C] [--enable|--disable]")
	}
	name := args[0]
	fs := flag.NewFlagSet("update", flag.ContinueOnError)
	message := fs.String("message", "", "new message")
	every := fs.String("every", "", "new interval")
	calendar := fs.String("calendar", "", "new OnCalendar expression")
	enable := fs.Bool("enable", false, "enable the timer")
	disable := fs.Bool("disable", false, "disable the timer")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	var msgP, everyP, calP *string
	var enabledP *bool
	if *message != "" {
		msgP = message
	}
	if *every != "" {
		everyP = every
	}
	if *calendar != "" {
		calP = calendar
	}
	if *enable {
		t := true
		enabledP = &t
	}
	if *disable {
		f := false
		enabledP = &f
	}
	l, err := updateLoop(name, msgP, everyP, calP, enabledP)
	if err != nil {
		return err
	}
	fmt.Printf("Updated loop %q (%s, enabled=%v). Timer: %s\n", l.Name, scheduleDesc(l), l.Enabled, timerStatus(l.Name))
	return nil
}
