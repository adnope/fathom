package collector

import (
	"bytes"
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
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
	if err := os.MkdirAll(filepath.Join(classDir, "eth0", "device"), 0755); err != nil {
		t.Fatalf("failed to create device marker: %v", err)
	}

	_ = os.WriteFile(filepath.Join(classDir, "eth0", "operstate"), []byte("up\n"), 0644)
	_ = os.WriteFile(filepath.Join(classDir, "eth0", "carrier"), []byte("1\n"), 0644)
	_ = os.WriteFile(filepath.Join(classDir, "eth0", "speed"), []byte("1000\n"), 0644)
	_ = os.WriteFile(filepath.Join(classDir, "eth0", "duplex"), []byte("full\n"), 0644)
	_ = os.WriteFile(filepath.Join(classDir, "eth0", "tx_queue_len"), []byte("1000\n"), 0644)

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
		if mdata["resource_id"] != "interface:eth0" || mdata["resource_type"] != "interface" {
			t.Errorf("expected interface resource labels, got %+v", mdata)
		}
		if mdata["interface_type"] != "ethernet" || mdata["operstate"] != "up" || mdata["carrier"] != 1 {
			t.Errorf("expected ethernet interface metadata in sample, got %+v", mdata)
		}
		if mdata["rx_bytes_total"] != uint64(50000) {
			t.Errorf("expected rx_bytes 50000, got %v", mdata["rx_bytes_total"])
		}
		if mdata["rx_bytes_per_second"] != nil || mdata["sample_interval_seconds"] != nil {
			t.Errorf("expected first-sample rates to be nil, got %+v", mdata)
		}
	})
}

func TestNetworkCollectorSkipsWiredOnlyIssuesForWireless(t *testing.T) {
	tmpDir := t.TempDir()
	netFile := filepath.Join(tmpDir, "net_dev")
	classDir := filepath.Join(tmpDir, "sys_net")
	wlanDir := filepath.Join(classDir, "wlan0")
	if err := os.MkdirAll(filepath.Join(wlanDir, "device"), 0755); err != nil {
		t.Fatalf("failed to create device marker: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(wlanDir, "wireless"), 0755); err != nil {
		t.Fatalf("failed to create wireless marker: %v", err)
	}
	_ = os.WriteFile(filepath.Join(wlanDir, "operstate"), []byte("up\n"), 0644)
	_ = os.WriteFile(filepath.Join(wlanDir, "carrier"), []byte("1\n"), 0644)
	_ = os.WriteFile(filepath.Join(wlanDir, "tx_queue_len"), []byte("1000\n"), 0644)

	content := `Inter-|   Receive                                                |  Transmit
 face |bytes    packets errs drop fifo frame compressed multicast|bytes    packets errs drop fifo colls carrier compressed
  wlan0: 50000 500 0 0 0 0 0 0 90000 900 0 0 0 0 0 0
`
	if err := os.WriteFile(netFile, []byte(content), 0644); err != nil {
		t.Fatalf("failed to write mock net_dev: %v", err)
	}

	var buf bytes.Buffer
	previousLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(previousLogger) })

	c := &NetworkCollector{
		procNetDevPath: netFile,
		sysClassNetDir: classDir,
		cachedMeta:     make(map[string]InterfaceMetadata),
		prevMetrics:    make(map[string]InterfaceMetrics),
		prevTime:       make(map[string]time.Time),
	}

	events, err := c.Collect(context.Background())
	if err != nil {
		t.Fatalf("Collect failed: %v", err)
	}

	var metricData map[string]any
	for _, event := range events {
		if event.Event == eventMetricSample {
			metricData = event.Data
		}
	}
	if metricData["interface_type"] != "wireless" {
		t.Fatalf("expected wireless interface type, got %+v", metricData)
	}
	if strings.Contains(buf.String(), "speed_mbps") || strings.Contains(buf.String(), "duplex") {
		t.Fatalf("expected no speed/duplex issue logs for wireless interface, got %s", buf.String())
	}
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

func TestCalculateNetworkRates(t *testing.T) {
	now := time.Unix(100, 0)
	prevTime := now.Add(-2 * time.Second)
	prev := InterfaceMetrics{
		rxBytes:   1000,
		txBytes:   2000,
		rxPackets: 10,
		txPackets: 20,
		rxErrors:  1,
		txErrors:  2,
		rxDropped: 3,
		txDropped: 4,
	}
	stats := rawNetDev{
		rxBytes:   3000,
		txBytes:   2600,
		rxPackets: 14,
		txPackets: 26,
		rxErrors:  3,
		txErrors:  4,
		rxDropped: 5,
		txDropped: 8,
	}

	rates := calculateNetworkRates(stats, prev, true, prevTime, true, now)

	if !rates.hasSampleInterval || rates.sampleIntervalSeconds != 2 {
		t.Fatalf("expected 2 second sample interval, got %+v", rates)
	}
	if rates.rxBytesRate != 1000 || rates.txBytesRate != 300 {
		t.Fatalf("unexpected byte rates: %+v", rates)
	}
	if rates.rxPacketsRate != 2 || rates.txPacketsRate != 3 {
		t.Fatalf("unexpected packet rates: %+v", rates)
	}
	if rates.rxErrorsRate != 1 || rates.txErrorsRate != 1 {
		t.Fatalf("unexpected error rates: %+v", rates)
	}
	if rates.rxDroppedRate != 1 || rates.txDroppedRate != 2 {
		t.Fatalf("unexpected drop rates: %+v", rates)
	}
}

func TestCalculateNetworkRatesCounterReset(t *testing.T) {
	now := time.Unix(100, 0)
	prevTime := now.Add(-2 * time.Second)
	prev := InterfaceMetrics{
		rxBytes:   1000,
		txBytes:   2000,
		rxPackets: 10,
		txPackets: 20,
		rxErrors:  1,
		txErrors:  2,
		rxDropped: 3,
		txDropped: 4,
	}

	rates := calculateNetworkRates(rawNetDev{}, prev, true, prevTime, true, now)

	if !rates.hasSampleInterval || rates.sampleIntervalSeconds != 2 {
		t.Fatalf("expected sample interval after counter reset, got %+v", rates)
	}
	if rates.rxBytesRate != 0 || rates.txBytesRate != 0 || rates.rxPacketsRate != 0 || rates.txPacketsRate != 0 {
		t.Fatalf("expected zero rates after counter reset, got %+v", rates)
	}
	if rates := calculateNetworkRates(rawNetDev{}, InterfaceMetrics{}, false, prevTime, true, now); rates != (networkRates{}) {
		t.Fatalf("expected zero rates without previous metrics, got %+v", rates)
	}
}

func TestParseProcNetDevLine(t *testing.T) {
	stats, ok := parseProcNetDevLine("  eth0: 50000 500 1 2 0 0 0 0 90000 900 3 4 0 0 0 0")
	if !ok {
		t.Fatal("expected net/dev line to parse")
	}
	if stats.iface != "eth0" {
		t.Fatalf("expected iface eth0, got %s", stats.iface)
	}
	if stats.rxBytes != 50000 || stats.rxPackets != 500 || stats.rxErrors != 1 || stats.rxDropped != 2 {
		t.Fatalf("unexpected rx stats: %+v", stats)
	}
	if stats.txBytes != 90000 || stats.txPackets != 900 || stats.txErrors != 3 || stats.txDropped != 4 {
		t.Fatalf("unexpected tx stats: %+v", stats)
	}

	if _, ok := parseProcNetDevLine("missing colon"); ok {
		t.Fatal("expected line without colon to be skipped")
	}
	if _, ok := parseProcNetDevLine("eth0: 1 2 3"); ok {
		t.Fatal("expected short stats line to be skipped")
	}
}

func TestBuildNetworkMetadataChangeEvents(t *testing.T) {
	oldCarrier := 1
	newCarrier := 0
	oldSpeed := 100
	newSpeed := 1000
	oldMeta := map[string]InterfaceMetadata{
		"eth1": {Interface: "eth1", Operstate: "up"},
		"eth2": {Interface: "eth2", Operstate: "up", Carrier: &oldCarrier, SpeedMbps: &oldSpeed},
		"eth3": {Interface: "eth3", Operstate: "up"},
	}
	currentMeta := map[string]InterfaceMetadata{
		"eth2": {Interface: "eth2", Operstate: "up", Carrier: &newCarrier, SpeedMbps: &newSpeed},
		"eth3": {Interface: "eth3", Operstate: "up"},
		"eth4": {Interface: "eth4", Operstate: "down"},
	}

	events := buildNetworkMetadataChangeEvents(oldMeta, currentMeta, []string{"eth2", "eth3", "eth4"})

	if len(events) != 3 {
		t.Fatalf("expected 3 metadata change events, got %d", len(events))
	}
	assertNetworkChangeEvent(t, events[0], "eth1", []string{"interface_removed"})
	assertNetworkChangeEvent(t, events[1], "eth2", []string{"carrier", "speed_mbps"})
	assertNetworkChangeEvent(t, events[2], "eth4", []string{"interface_added"})
}

func assertNetworkChangeEvent(t *testing.T, event Event, iface string, changedFields []string) {
	t.Helper()

	if event.Event != "network_metadata_changed" || event.Collector != "network" {
		t.Fatalf("unexpected event headers: %+v", event)
	}
	if event.Data["interface"] != iface {
		t.Fatalf("expected interface %s, got %v", iface, event.Data["interface"])
	}
	if got := event.Data["changed_fields"].([]string); !slices.Equal(got, changedFields) {
		t.Fatalf("expected changed fields %v, got %v", changedFields, got)
	}
}
