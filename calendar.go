package main

import (
	"fmt"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"
)

// A pure-Go OnCalendar parser for the portable backend.
//
// The systemd backend answers validateCalendar by shelling out to
// `systemd-analyze calendar`, and computing the next fire is systemd's problem
// after that. Neither is available in a container with no systemd, so this file
// does both: parse an OnCalendar expression into candidate sets, and find the
// first instant after a given time that matches.
//
// It implements a SUBSET, and the subset is the point. Silently accepting an
// expression we then mis-schedule is far worse than refusing it — a loop that
// fires at the wrong time still looks alive, so nobody investigates. Every
// parse failure names the supported forms (see calendarHelp) and createLoop
// rejects at create time, when there is still a human reading the error.
//
// Supported:
//
//	minutely | hourly | daily | weekly | monthly | yearly | annually
//	[WEEKDAYS] [DATE] TIME
//	  WEEKDAYS  Mon | Mon,Wed | Mon..Fri | Mon..Fri,Sun   (case-insensitive)
//	  DATE      Y-M-D, each component * or a number       (e.g. *-*-*, *-*-01)
//	  TIME      H:M or H:M:S, each component * or a number
//	Every component also takes a list (1,2), a range (1..5) and a step
//	(*/15, 0..30/10, 9/2). Omitting DATE means every day; omitting TIME is
//	not allowed, because "Mon" alone is a schedule nobody can read.
//
// Not supported: timezone suffixes, ~ (last day of month), UTC/local overrides,
// unix timestamps (@...), and the sub-second field. Everything unsupported is
// rejected, loudly.
//
// Times are LOCAL, like systemd's OnCalendar. A standup expression of 09:00
// that fired at 02:00 because everloop reasoned in UTC would be a bug nobody
// would think to report as a bug.

const calendarHelp = `supported: minutely, hourly, daily, weekly, monthly, yearly, ` +
	`or "[Mon..Fri] [*-*-*] HH:MM[:SS]" where every component takes *, a number, ` +
	`a list (1,2), a range (1..5) or a step (*/15)`

// maxCalendarSearchYears bounds the walk for the next occurrence. It also
// makes "never happens" a create-time error rather than a loop that quietly
// never fires: *-02-30 parses fine and matches nothing, forever.
const maxCalendarSearchYears = 10

// calSpec is an OnCalendar expression as candidate sets, each sorted ascending
// so the first match found walking them in order is the earliest one. A nil
// set means "any value" — for date components that is cheaper than
// materialising every year, and for weekdays it is the common case.
type calSpec struct {
	weekdays []int // time.Weekday values: Sunday=0
	years    []int
	months   []int
	days     []int
	hours    []int
	minutes  []int
	seconds  []int
}

// systemd's weekday names and its Mon=1..Sun=7 ordering, which is what makes
// "Sat..Sun" a two-day range instead of an empty one.
var calWeekdays = map[string]int{
	"mon": 1, "monday": 1,
	"tue": 2, "tuesday": 2,
	"wed": 3, "wednesday": 3,
	"thu": 4, "thursday": 4,
	"fri": 5, "friday": 5,
	"sat": 6, "saturday": 6,
	"sun": 7, "sunday": 7,
}

// calShorthands are systemd's named expressions, expanded to their documented
// equivalents rather than special-cased downstream.
var calShorthands = map[string]string{
	"minutely":   "*-*-* *:*:00",
	"hourly":     "*-*-* *:00:00",
	"daily":      "*-*-* 00:00:00",
	"weekly":     "Mon *-*-* 00:00:00",
	"monthly":    "*-*-01 00:00:00",
	"yearly":     "*-01-01 00:00:00",
	"annually":   "*-01-01 00:00:00",
	"quarterly":  "*-01,04,07,10-01 00:00:00",
	"semiannual": "*-01,07-01 00:00:00",
}

func calendarError(expr, why string) error {
	return fmt.Errorf("invalid OnCalendar expression %q: %s (%s)", expr, why, calendarHelp)
}

// parseCalendarSpec turns an OnCalendar expression into candidate sets.
func parseCalendarSpec(expr string) (calSpec, error) {
	raw := strings.TrimSpace(expr)
	if raw == "" {
		return calSpec{}, calendarError(expr, "empty")
	}
	if full, ok := calShorthands[strings.ToLower(raw)]; ok {
		raw = full
	}

	var spec calSpec
	fields := strings.Fields(raw)

	// A leading field containing a letter can only be a weekday spec; anything
	// else with letters in it is one of the forms we do not support (timezone
	// names, @timestamps) and must not be silently ignored.
	if strings.ContainsFunc(fields[0], func(r rune) bool { return r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' }) {
		wd, err := parseWeekdays(fields[0])
		if err != nil {
			return calSpec{}, calendarError(expr, err.Error())
		}
		spec.weekdays, fields = wd, fields[1:]
	}

	var date, clock string
	switch len(fields) {
	case 1:
		if !strings.Contains(fields[0], ":") {
			return calSpec{}, calendarError(expr, "a time of day is required")
		}
		clock = fields[0]
	case 2:
		date, clock = fields[0], fields[1]
	default:
		return calSpec{}, calendarError(expr, "expected [weekday] [date] time")
	}

	if date != "" {
		parts := strings.Split(date, "-")
		if len(parts) != 3 {
			return calSpec{}, calendarError(expr, "date must be Y-M-D")
		}
		var err error
		if spec.years, err = parseCalField(parts[0], 1970, 2200); err != nil {
			return calSpec{}, calendarError(expr, "year: "+err.Error())
		}
		if spec.months, err = parseCalField(parts[1], 1, 12); err != nil {
			return calSpec{}, calendarError(expr, "month: "+err.Error())
		}
		if spec.days, err = parseCalField(parts[2], 1, 31); err != nil {
			return calSpec{}, calendarError(expr, "day: "+err.Error())
		}
	}

	parts := strings.Split(clock, ":")
	if len(parts) != 2 && len(parts) != 3 {
		return calSpec{}, calendarError(expr, "time must be HH:MM or HH:MM:SS")
	}
	var err error
	if spec.hours, err = parseCalField(parts[0], 0, 23); err != nil {
		return calSpec{}, calendarError(expr, "hour: "+err.Error())
	}
	if spec.minutes, err = parseCalField(parts[1], 0, 59); err != nil {
		return calSpec{}, calendarError(expr, "minute: "+err.Error())
	}
	// systemd defaults the seconds field to 0, not to "every second": `09:00`
	// means once at nine, not sixty times.
	spec.seconds = []int{0}
	if len(parts) == 3 {
		if spec.seconds, err = parseCalField(parts[2], 0, 59); err != nil {
			return calSpec{}, calendarError(expr, "second: "+err.Error())
		}
	}
	return spec, nil
}

// parseWeekdays parses "Mon", "Mon,Wed", "Mon..Fri" (and combinations) into
// time.Weekday values.
func parseWeekdays(s string) ([]int, error) {
	var out []int
	for _, term := range strings.Split(s, ",") {
		lo, hi := term, term
		if a, b, ok := strings.Cut(term, ".."); ok {
			lo, hi = a, b
		}
		start, ok := calWeekdays[strings.ToLower(strings.TrimSpace(lo))]
		if !ok {
			return nil, fmt.Errorf("unknown weekday %q", lo)
		}
		end, ok := calWeekdays[strings.ToLower(strings.TrimSpace(hi))]
		if !ok {
			return nil, fmt.Errorf("unknown weekday %q", hi)
		}
		if end < start {
			return nil, fmt.Errorf("weekday range %q runs backwards", term)
		}
		for d := start; d <= end; d++ {
			out = append(out, d%7) // systemd Sun=7 -> time.Sunday=0
		}
	}
	return dedupe(out), nil
}

// parseCalField parses one component: "*", "5", "1,2,3", "1..5", "*/15",
// "0..30/10", "9/2". Returns nil for "*" (any value), which the matcher reads
// as "no constraint".
func parseCalField(s string, min, max int) ([]int, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil, fmt.Errorf("empty component")
	}
	if s == "*" {
		return nil, nil
	}
	var out []int
	for _, term := range strings.Split(s, ",") {
		body, stepStr, hasStep := strings.Cut(term, "/")
		step := 1
		if hasStep {
			n, err := strconv.Atoi(stepStr)
			if err != nil || n < 1 {
				return nil, fmt.Errorf("bad step in %q", term)
			}
			step = n
		}
		lo, hi := min, max
		if body != "*" {
			a, b, isRange := strings.Cut(body, "..")
			n, err := parseCalNumber(a, min, max)
			if err != nil {
				return nil, err
			}
			switch {
			case isRange:
				m, err := parseCalNumber(b, min, max)
				if err != nil {
					return nil, err
				}
				if m < n {
					return nil, fmt.Errorf("range %q runs backwards", body)
				}
				lo, hi = n, m
			case hasStep:
				// systemd's "9/2" means "from 9, every 2, to the end".
				lo, hi = n, max
			default:
				lo, hi = n, n
			}
		}
		for v := lo; v <= hi; v += step {
			out = append(out, v)
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("matches nothing")
	}
	return dedupe(out), nil
}

func parseCalNumber(s string, min, max int) (int, error) {
	n, err := strconv.Atoi(strings.TrimSpace(s))
	if err != nil {
		return 0, fmt.Errorf("%q is not a number", s)
	}
	if n < min || n > max {
		return 0, fmt.Errorf("%d out of range %d..%d", n, min, max)
	}
	return n, nil
}

func dedupe(v []int) []int {
	sort.Ints(v)
	return slices.Compact(v)
}

func matches(set []int, v int) bool { return set == nil || slices.Contains(set, v) }

func (s calSpec) matchesDate(t time.Time) bool {
	return matches(s.years, t.Year()) &&
		matches(s.months, int(t.Month())) &&
		matches(s.days, t.Day()) &&
		matches(s.weekdays, int(t.Weekday()))
}

// nextCalendar returns the first local time matching expr strictly after
// `after`, walking day by day and then the time-of-day candidates in order.
// Brute force over at most ~3650 days costs microseconds and, unlike closed-form
// arithmetic, cannot quietly disagree with matchesDate about a leap day.
func nextCalendar(expr string, after time.Time) (time.Time, error) {
	spec, err := parseCalendarSpec(expr)
	if err != nil {
		return time.Time{}, err
	}
	// Whole seconds only: the candidate sets have no sub-second component, so a
	// fire computed for :00.000 must not be considered "already past" because
	// `after` carried nanoseconds.
	from := after.In(time.Local).Truncate(time.Second).Add(time.Second)
	day := time.Date(from.Year(), from.Month(), from.Day(), 0, 0, 0, 0, time.Local)

	hours, minutes, seconds := spec.hours, spec.minutes, spec.seconds
	if hours == nil {
		hours = seq(0, 23)
	}
	if minutes == nil {
		minutes = seq(0, 59)
	}
	if seconds == nil {
		seconds = seq(0, 59)
	}

	for i := 0; i < maxCalendarSearchYears*366; i++ {
		d := day.AddDate(0, 0, i)
		if !spec.matchesDate(d) {
			continue
		}
		for _, h := range hours {
			for _, m := range minutes {
				for _, sec := range seconds {
					t := time.Date(d.Year(), d.Month(), d.Day(), h, m, sec, 0, time.Local)
					if !t.Before(from) {
						return t, nil
					}
				}
			}
		}
	}
	return time.Time{}, fmt.Errorf("OnCalendar expression %q has no occurrence in the next %d years",
		expr, maxCalendarSearchYears)
}

func seq(lo, hi int) []int {
	out := make([]int, 0, hi-lo+1)
	for v := lo; v <= hi; v++ {
		out = append(out, v)
	}
	return out
}
