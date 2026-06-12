package collector

import (
	"strings"
	"testing"
)

func TestGetHostMetadata(t *testing.T) {
	ev := GetHostMetadata()

	if ev.Event != "host_metadata" {
		t.Errorf("expected event host_metadata, got %s", ev.Event)
	}

	if ev.Component != "agent" {
		t.Errorf("expected component agent, got %s", ev.Component)
	}

	if ev.Data["os"] != "linux" {
		t.Errorf("expected os to be linux, got %v", ev.Data["os"])
	}

	if _, ok := ev.Data["hostname"]; !ok {
		t.Error("expected hostname in host_metadata")
	}

	if _, ok := ev.Data["kernel"]; !ok {
		t.Error("expected kernel in host_metadata")
	}

	if _, ok := ev.Data["arch"]; !ok {
		t.Error("expected arch in host_metadata")
	}
}

func TestParseOSRelease(t *testing.T) {
	osRelease, err := parseOSRelease(strings.NewReader(`NAME="Example Linux"
PRETTY_NAME="Example Linux 1.0"
`))
	if err != nil {
		t.Fatalf("parseOSRelease failed: %v", err)
	}
	if osRelease != "Example Linux 1.0" {
		t.Fatalf("expected pretty name, got %q", osRelease)
	}

	osRelease, err = parseOSRelease(strings.NewReader(`NAME="Example Linux"`))
	if err != nil {
		t.Fatalf("parseOSRelease failed: %v", err)
	}
	if osRelease != "" {
		t.Fatalf("expected missing pretty name to return empty string, got %q", osRelease)
	}
}

func TestParseCPUTopology(t *testing.T) {
	cpuInfo := `processor   : 0
physical id : 0
core id     : 0

processor   : 1
physical id : 0
core id     : 1

processor   : 2
physical id : 1
core id     : 0
`

	sockets, physicalCores := parseCPUTopology(strings.NewReader(cpuInfo))

	if sockets != 2 {
		t.Fatalf("expected 2 sockets, got %d", sockets)
	}
	if physicalCores != 3 {
		t.Fatalf("expected 3 physical cores, got %d", physicalCores)
	}
}

func TestParseBootTime(t *testing.T) {
	bootTime, err := parseBootTime(strings.NewReader(`cpu  1 2 3 4
btime 1710000000
`))
	if err != nil {
		t.Fatalf("parseBootTime failed: %v", err)
	}
	if bootTime != 1710000000 {
		t.Fatalf("expected boot time 1710000000, got %d", bootTime)
	}
}
