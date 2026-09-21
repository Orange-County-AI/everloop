//go:build linux

package main

import (
	"strings"
	"testing"
)

// A loop created with --instance must still fire into that instance when the
// timer runs it minutes later, with no flag and no inherited environment. The
// generated unit is the only thing carrying that across, so it has to bake the
// instance in whichever way the caller selected it.
func TestGeneratedUnitBakesTheInstanceFromTheFlag(t *testing.T) {
	newInstanceHome(t)
	parseGlobalFlags(t, "--instance", "jessica", "create", "covers")

	service, timer, err := renderUnits(&Loop{Name: "covers", Message: "hi", Every: "5m", Enabled: true}, "/usr/local/bin/everloop")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(service, "Environment=EVERLOOP_INSTANCE=jessica\n") {
		t.Fatalf("service unit does not carry the instance:\n%s", service)
	}
	if !strings.Contains(service, "ExecStart=/usr/local/bin/everloop tick covers\n") {
		t.Fatalf("unexpected ExecStart:\n%s", service)
	}
	if !strings.Contains(timer, "OnUnitActiveSec=300s") {
		t.Fatalf("unexpected timer:\n%s", timer)
	}
}

func TestGeneratedUnitOmitsTheInstanceByDefault(t *testing.T) {
	newInstanceHome(t)
	parseGlobalFlags(t, "create", "covers")

	service, _, err := renderUnits(&Loop{Name: "covers", Message: "hi", Every: "5m", Enabled: true}, "/usr/local/bin/everloop")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(service, "EVERLOOP_INSTANCE") {
		t.Fatalf("default instance leaked an Environment= line:\n%s", service)
	}
}
