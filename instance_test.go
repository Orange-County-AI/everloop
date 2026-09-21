package main

// Instance-selection tests. The instance is the only thing standing between
// two concurrent orchestrators and each other's spool, so the rules it obeys —
// the flag beats the environment, an invalid name is refused before anything
// touches disk, and a tick lands in exactly one instance's queue — are worth
// pinning down rather than re-deriving from instanceName().

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// parseGlobalFlags runs the argv split the way main() does and restores the
// package-level instance state afterwards, so tests never leak into each other.
func parseGlobalFlags(t *testing.T, argv ...string) []string {
	t.Helper()
	prevVal, prevSet := instanceFlag, instanceFlagSet
	t.Cleanup(func() { instanceFlag, instanceFlagSet = prevVal, prevSet })
	instanceFlag, instanceFlagSet = "", false
	rest, err := splitInstanceFlag(argv)
	if err != nil {
		t.Fatalf("splitInstanceFlag(%q): %v", argv, err)
	}
	return rest
}

func TestInstanceFlagBeatsEnv(t *testing.T) {
	t.Setenv("EVERLOOP_INSTANCE", "from-env")
	rest := parseGlobalFlags(t, "--instance", "from-flag", "list")
	if got := instanceName(); got != "from-flag" {
		t.Fatalf("instanceName() = %q, want the flag value %q", got, "from-flag")
	}
	if len(rest) != 1 || rest[0] != "list" {
		t.Fatalf("remaining args = %q, want [list]", rest)
	}
}

func TestInstanceEnvAloneWorks(t *testing.T) {
	t.Setenv("EVERLOOP_INSTANCE", "jessica")
	rest := parseGlobalFlags(t, "list")
	if got := instanceName(); got != "jessica" {
		t.Fatalf("instanceName() = %q, want %q", got, "jessica")
	}
	if err := checkInstance(); err != nil {
		t.Fatalf("checkInstance() = %v, want nil", err)
	}
	if len(rest) != 1 || rest[0] != "list" {
		t.Fatalf("remaining args = %q, want [list]", rest)
	}
}

// The flag is global: it must work wherever it lands, because an .mcp.json
// entry writes `["serve", "--instance", "jessica"]` while a human types
// `everloop --instance jessica list`.
func TestInstanceFlagIsPositionIndependent(t *testing.T) {
	t.Setenv("EVERLOOP_INSTANCE", "")
	cases := []struct {
		argv []string
		want []string
	}{
		{[]string{"--instance", "jessica", "serve"}, []string{"serve"}},
		{[]string{"serve", "--instance", "jessica"}, []string{"serve"}},
		{[]string{"serve", "--instance=jessica"}, []string{"serve"}},
		{[]string{"-instance", "jessica", "serve"}, []string{"serve"}},
		{[]string{"tick", "--instance", "jessica", "covers"}, []string{"tick", "covers"}},
		{[]string{"update", "covers", "--instance=jessica", "--every", "5m"}, []string{"update", "covers", "--every", "5m"}},
	}
	for _, tc := range cases {
		rest := parseGlobalFlags(t, tc.argv...)
		if instanceName() != "jessica" {
			t.Errorf("%q: instanceName() = %q, want jessica", tc.argv, instanceName())
		}
		if strings.Join(rest, "\x00") != strings.Join(tc.want, "\x00") {
			t.Errorf("%q: remaining args = %q, want %q", tc.argv, rest, tc.want)
		}
	}
}

// `--instance ""` is a deliberate "the default instance", not an absent flag,
// so it has to override an inherited EVERLOOP_INSTANCE too.
func TestEmptyInstanceFlagOverridesEnv(t *testing.T) {
	t.Setenv("EVERLOOP_INSTANCE", "jessica")
	parseGlobalFlags(t, "--instance=", "list")
	if got := instanceName(); got != "" {
		t.Fatalf("instanceName() = %q, want the default instance", got)
	}
	if err := checkInstance(); err != nil {
		t.Fatalf("checkInstance() = %v, want nil", err)
	}
}

// Nothing that is not exactly --instance may be swallowed: everything else
// belongs to the subcommand's own parser.
func TestSplitInstanceFlagLeavesOtherArgsAlone(t *testing.T) {
	t.Setenv("EVERLOOP_INSTANCE", "")
	argv := []string{"create", "covers", "--message", "--instances-of-this", "--instance-ish", "--", "--instance", "x"}
	rest := parseGlobalFlags(t, argv...)
	if instanceFlagSet {
		t.Fatalf("instance flag set by %q", argv)
	}
	if strings.Join(rest, "\x00") != strings.Join(argv, "\x00") {
		t.Fatalf("remaining args = %q, want them untouched", rest)
	}
}

func TestInstanceFlagNeedsAValue(t *testing.T) {
	if _, err := splitInstanceFlag([]string{"list", "--instance"}); err == nil {
		t.Fatal("trailing --instance accepted, want an error")
	}
}

func TestInvalidInstanceIsRejected(t *testing.T) {
	t.Run("flag", func(t *testing.T) {
		t.Setenv("EVERLOOP_INSTANCE", "")
		parseGlobalFlags(t, "--instance", "Bad_Name", "list")
		err := checkInstance()
		if err == nil {
			t.Fatal("checkInstance() = nil, want an error for Bad_Name")
		}
		if !strings.Contains(err.Error(), "--instance") {
			t.Fatalf("error %q does not name the flag the caller typed", err)
		}
	})
	t.Run("env", func(t *testing.T) {
		t.Setenv("EVERLOOP_INSTANCE", "../escape")
		parseGlobalFlags(t, "list")
		err := checkInstance()
		if err == nil {
			t.Fatal("checkInstance() = nil, want an error for ../escape")
		}
		if !strings.Contains(err.Error(), "EVERLOOP_INSTANCE") {
			t.Fatalf("error %q does not name the env var the caller set", err)
		}
	})
}

// newInstanceHome points the data dir at a scratch HOME (not EVERLOOP_DATA_DIR,
// which overrides the instance path wholesale) so instance isolation is the
// thing under test.
func newInstanceHome(t *testing.T) string {
	t.Helper()
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("EVERLOOP_DATA_DIR", "")
	t.Setenv("EVERLOOP_INSTANCE", "")
	return home
}

func TestInstanceFlagIsolatesDataDirAndUnitNames(t *testing.T) {
	home := newInstanceHome(t)
	base := filepath.Join(home, ".local", "share", "everloop")

	parseGlobalFlags(t, "list")
	if got := dataDir(); got != base {
		t.Fatalf("default dataDir() = %q, want %q", got, base)
	}

	parseGlobalFlags(t, "--instance", "jessica", "list")
	if got, want := dataDir(), filepath.Join(base, "jessica"); got != want {
		t.Fatalf("dataDir() = %q, want %q", got, want)
	}
	if got, want := timerName("covers"), "everloop-jessica-covers.timer"; got != want {
		t.Fatalf("timerName() = %q, want %q", got, want)
	}
}

// The round trip that matters: a tick spooled under one instance must be
// claimable by that instance's `serve` drain and invisible to another's.
func TestTickAndDrainIsolateByInstance(t *testing.T) {
	newInstanceHome(t)

	// `everloop create ... --instance jessica` (minus the real timer install).
	parseGlobalFlags(t, "--instance", "jessica", "create", "covers")
	if err := checkInstance(); err != nil {
		t.Fatal(err)
	}
	if err := saveLoop(&Loop{Name: "covers", Message: "look at the covers", Every: "5m", Enabled: true}); err != nil {
		t.Fatal(err)
	}
	jessicaDir := dataDir()

	// `everloop tick covers` as the generated unit runs it: EVERLOOP_INSTANCE
	// baked into the unit rather than a flag, same instance either way.
	parseGlobalFlags(t, "tick", "covers")
	t.Setenv("EVERLOOP_INSTANCE", "jessica")
	if got := dataDir(); got != jessicaDir {
		t.Fatalf("env-selected dataDir() = %q, want the flag's %q", got, jessicaDir)
	}
	if err := enqueueTick("covers"); err != nil {
		t.Fatal(err)
	}

	// A second instance shares neither the loop nor the spool.
	parseGlobalFlags(t, "--instance", "clem", "list")
	if loops, err := listLoops(); err != nil || len(loops) != 0 {
		t.Fatalf("instance clem sees loops %v (err %v), want none", loops, err)
	}
	if err := enqueueTick("covers"); err == nil {
		t.Fatal("tick of another instance's loop succeeded, want a not-found error")
	}
	if claimed, err := claimPending(); err != nil || len(claimed) != 0 {
		t.Fatalf("instance clem claimed %d messages (err %v), want 0", len(claimed), err)
	}

	// The drain `serve` performs, back in the instance that spooled it.
	parseGlobalFlags(t, "--instance", "jessica", "serve")
	pending, err := claimPending()
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 1 {
		t.Fatalf("claimed %d messages, want 1", len(pending))
	}
	if pending[0].msg.Content != "look at the covers" || pending[0].msg.Loop != "covers" {
		t.Fatalf("unexpected message: %+v", pending[0].msg)
	}
	if !strings.HasPrefix(pending[0].path, jessicaDir) {
		t.Fatalf("claimed from %q, want a path under %q", pending[0].path, jessicaDir)
	}
	pending[0].ack()

	if _, err := os.Stat(pending[0].path); !os.IsNotExist(err) {
		t.Fatalf("ack left %q behind", pending[0].path)
	}
}
