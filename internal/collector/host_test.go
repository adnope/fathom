package collector

import (
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
