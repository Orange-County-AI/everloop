package main

// OnCalendar tests. The portable backend cannot ask systemd-analyze whether an
// expression is valid, so this parser IS the validation — and the failure it
// has to prevent is not a crash but a loop that fires at the wrong time, which
// looks alive and gets investigated by nobody. Hence a table of exact next-fire
// instants rather than "it parsed", and an equally deliberate table of things
// that must be refused.

import (
	"strings"
	"testing"
	"time"
)

func localTime(t *testing.T, s string) time.Time {
	t.Helper()
	ts, err := time.ParseInLocation("2006-01-02 15:04:05", s, time.Local)
	if err != nil {
		t.Fatal(err)
	}
	return ts
}

func TestNextCalendarComputesTheNextFire(t *testing.T) {
	// 2026-07-27 is a Monday, 2026-07-31 a Friday, 2026-08-01 a Saturday.
	cases := []struct{ expr, after, want string }{
		{"minutely", "2026-07-27 09:15:30", "2026-07-27 09:16:00"},
		{"hourly", "2026-07-27 09:15:00", "2026-07-27 10:00:00"},
		{"daily", "2026-07-27 09:15:00", "2026-07-28 00:00:00"},
		{"weekly", "2026-07-27 09:15:00", "2026-08-03 00:00:00"},
		{"monthly", "2026-07-27 09:15:00", "2026-08-01 00:00:00"},
		{"yearly", "2026-07-27 09:15:00", "2027-01-01 00:00:00"},

		{"*-*-* 09:00:00", "2026-07-27 09:15:00", "2026-07-28 09:00:00"},
		{"*-*-* 09:00:00", "2026-07-27 08:00:00", "2026-07-27 09:00:00"},
		{"09:00", "2026-07-27 08:00:00", "2026-07-27 09:00:00"},
		{"17:30:15", "2026-07-27 17:30:14", "2026-07-27 17:30:15"},

		// Weekday specs, the form our standing sweeps actually use.
		{"Mon..Fri 09:00", "2026-07-31 10:00:00", "2026-08-03 09:00:00"},
		{"Mon..Fri 09:00", "2026-07-27 08:59:59", "2026-07-27 09:00:00"},
		{"Sat,Sun 12:00", "2026-07-27 09:00:00", "2026-08-01 12:00:00"},
		{"Fri *-*-* 18:00:00", "2026-07-27 09:00:00", "2026-07-31 18:00:00"},

		// Lists, ranges and steps in any component.
		{"*:0/15", "2026-07-27 09:16:00", "2026-07-27 09:30:00"},
		{"*:0,30", "2026-07-27 09:31:00", "2026-07-27 10:00:00"},
		{"9..17:00", "2026-07-27 09:30:00", "2026-07-27 10:00:00"},
		{"*-*-01 00:00:00", "2026-07-27 09:00:00", "2026-08-01 00:00:00"},
		{"2026-12-25 06:30:00", "2026-07-27 09:00:00", "2026-12-25 06:30:00"},

		// A fire exactly now is in the past: next means next.
		{"*-*-* 09:00:00", "2026-07-27 09:00:00", "2026-07-28 09:00:00"},
	}
	for _, c := range cases {
		got, err := nextCalendar(c.expr, localTime(t, c.after))
		if err != nil {
			t.Errorf("%q after %s: %v", c.expr, c.after, err)
			continue
		}
		if want := localTime(t, c.want); !got.Equal(want) {
			t.Errorf("%q after %s = %s, want %s", c.expr, c.after, got, want)
		}
	}
}

// Sub-second junk on the input clock must not push a fire computed for :00 into
// "already past" — the scheduler compares against wall-clock times that always
// carry nanoseconds.
func TestNextCalendarIgnoresSubSecondPrecision(t *testing.T) {
	after := localTime(t, "2026-07-27 08:59:59").Add(999 * time.Millisecond)
	got, err := nextCalendar("09:00", after)
	if err != nil {
		t.Fatal(err)
	}
	if want := localTime(t, "2026-07-27 09:00:00"); !got.Equal(want) {
		t.Fatalf("got %s, want %s", got, want)
	}
}

func TestUnsupportedCalendarExpressionsAreRefused(t *testing.T) {
	// Every one of these is something systemd or a user might plausibly write.
	// Accepting any of them silently would mis-schedule a loop rather than fail.
	for _, expr := range []string{
		"",
		"Mon",                    // no time of day
		"every 5 minutes",        // English, not OnCalendar
		"17 * * * *",             // cron, a different language entirely
		"*-*-* 09:00:00 UTC",     // timezone suffix
		"*-*-* 25:00:00",         // hour out of range
		"*-*-* 09:60:00",         // minute out of range
		"*-*-~01 09:00:00",       // systemd's last-day-of-month syntax
		"*-*-* 09:00:00 Europe/", // trailing junk
		"@1780000000",            // unix timestamp form
		"Funday 09:00",           // not a weekday
		"Fri..Mon 09:00",         // backwards weekday range
		"*-*-* 09:00:00:00",      // four time components
		"*-* 09:00",              // two-component date
	} {
		if err := (portable{}).validateCalendar(expr); err == nil {
			t.Errorf("accepted unsupported expression %q", expr)
		} else if !strings.Contains(err.Error(), "supported:") {
			t.Errorf("rejection of %q does not say what IS supported: %v", expr, err)
		}
	}
}

// An expression that parses but never happens is the worst case of all: the
// loop looks installed and simply never fires. Refuse it at create time.
func TestCalendarThatNeverOccursIsRefused(t *testing.T) {
	if err := (portable{}).validateCalendar("*-02-30 09:00:00"); err == nil {
		t.Fatal("accepted a date that never occurs")
	}
}

func TestParseCalFieldComponents(t *testing.T) {
	cases := []struct {
		in   string
		want []int
	}{
		{"*", nil},
		{"5", []int{5}},
		{"1,3,5", []int{1, 3, 5}},
		{"1..4", []int{1, 2, 3, 4}},
		{"*/15", []int{0, 15, 30, 45}},
		{"0..30/10", []int{0, 10, 20, 30}},
		{"45/10", []int{45, 55}},
		{"5,1,5", []int{1, 5}}, // deduped and sorted, so the earliest match is first
	}
	for _, c := range cases {
		got, err := parseCalField(c.in, 0, 59)
		if err != nil {
			t.Errorf("%q: %v", c.in, err)
			continue
		}
		if len(got) != len(c.want) {
			t.Errorf("%q = %v, want %v", c.in, got, c.want)
			continue
		}
		for i := range got {
			if got[i] != c.want[i] {
				t.Errorf("%q = %v, want %v", c.in, got, c.want)
				break
			}
		}
	}
}
