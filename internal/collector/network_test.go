package collector

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"fathom/internal/config"
)

func TestNetworkCollector(t *testing.T) {
	tmpDir := t.TempDir()
	netFile := filepath.Join(tmpDir, "net_dev")
	classDir := filepath.Join(tmpDir, "sys_net")

	err := os.MkdirAll(filepath.Join(classDir, "eth0"), 0755)
	if err != nil {
		t.Fatalf("failed to create sys class net mock: %v", err)
	}

	_ = os.WriteFile(filepath.Join(classDir, "eth0", "operstate"), []byte("up\n"), 0644)
	_ = os.WriteFile(filepath.Join(classDir, "eth0", "carrier"), []byte("1\n"), 0644)
	_ = os.WriteFile(filepath.Join(classDir, "eth0", "speed"), []byte("1000\n"), 0644)

	content := `Inter-|   Receive                                                |  Transmit
 face |bytes    packets errs drop fifo frame compressed multicast|bytes    packets errs drop fifo colls carrier compressed
    lo: 1000 10 0 0 0 0 0 0 1000 10 0 0 0 0 0 0
  eth0: 50000 500 0 0 0 0 0 0 90000 900 0 0 0 0 0 0
`
	err = os.WriteFile(netFile, []byte(content), 0644)
	if err != nil {
		t.Fatalf("failed to write mock net_dev: %v", err)
	}

	t.Run("snapshot and metrics collection", func(t *testing.T) {
		includeVirtual := true
		c := &NetworkCollector{
			procNetDevPath: netFile,
			sysClassNetDir: classDir,
			cachedMeta:     make(map[string]InterfaceMetadata),
			prevMetrics:    make(map[string]InterfaceMetrics),
			prevTime:       make(map[string]time.Time),
			config: &config.NetworkConfig{
				IncludeVirtual: &includeVirtual,
			},
		}

		events, err := c.Collect(context.Background())
		if err != nil {
			t.Fatalf("Collect failed: %v", err)
		}

		if len(events) != 2 {
			t.Fatalf("expected 2 events, got %d", len(events))
		}

		var snapshotEv, metricEv Event
		for _, ev := range events {
			switch ev.Event {
			case "network_metadata_snapshot":
				snapshotEv = ev
			case "metric_sample":
				metricEv = ev
			}
		}

		if snapshotEv.Event == "" {
			t.Error("missing network_metadata_snapshot event")
		}
		if metricEv.Event == "" {
			t.Error("missing metric_sample event")
		}

		data := snapshotEv.Data
		ifaces := data["interfaces"].([]any)
		if len(ifaces) != 1 {
			t.Errorf("expected 1 interface metadata in snapshot, got %d", len(ifaces))
		}

		mdata := metricEv.Data
		if mdata["interface"] != "eth0" {
			t.Errorf("expected eth0 interface name, got %v", mdata["interface"])
		}
		if mdata["rx_bytes_total"] != uint64(50000) {
			t.Errorf("expected rx_bytes 50000, got %v", mdata["rx_bytes_total"])
		}
	})
}

func TestIPLookupFallbacks(t *testing.T) {
	// Test getIPv4ViaIoctl on the standard loopback interface
	ip4, err := getIPv4ViaIoctl("lo")
	if err != nil {
		t.Logf("getIPv4ViaIoctl failed (normal if not supported on this platform/kernel): %v", err)
	} else if ip4 != "127.0.0.1" {
		t.Errorf("expected 127.0.0.1 for loopback, got %s", ip4)
	}

	// Test getIPv6FromProc on the standard loopback interface
	ip6, err := getIPv6FromProc("lo")
	if err != nil {
		t.Logf("getIPv6FromProc failed: %v", err)
	} else if ip6 != "::1" && ip6 != "0:0:0:0:0:0:0:1" && ip6 != "::" {
		t.Errorf("expected ::1 for loopback, got %s", ip6)
	}

	// Test getIPsFallbackList
	ip4Fallback, ip6Fallback, err := getIPsFallbackList("lo")
	if err != nil {
		t.Fatalf("getIPsFallbackList failed for loopback: %v", err)
	}
	if ip4Fallback == nil || *ip4Fallback != "127.0.0.1" {
		t.Errorf("expected 127.0.0.1 for loopback fallback list, got %v", ip4Fallback)
	}
	if ip6Fallback == nil || (*ip6Fallback != "::1" && *ip6Fallback != "0:0:0:0:0:0:0:1") {
		t.Logf("loopback fallback list IPv6: %v (can be nil if IPv6 is disabled for loopback)", ip6Fallback)
	}
}
