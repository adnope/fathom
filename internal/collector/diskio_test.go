package collector

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestDiskIOCollector(t *testing.T) {
	tmpDir := t.TempDir()
	diskstatsPath := filepath.Join(tmpDir, "diskstats")
	mountsPath := filepath.Join(tmpDir, "mounts")

	// Mock diskstats on sample 1
	sample1 := `   8       0 sda 100 10 200 1000 50 5 100 500 0 120 1200
 259       0 nvme0n1 200 20 400 2000 60 6 120 600 0 150 1500
   7       0 loop0 99 99 99 99 99 99 99 99 99 99 99
 253       0 dm-0 150 15 300 1500 40 4 80 400 0 90 900
`
	if err := os.WriteFile(diskstatsPath, []byte(sample1), 0644); err != nil {
		t.Fatalf("failed to write mock diskstats: %v", err)
	}
	if err := os.WriteFile(mountsPath, []byte("/dev/sda /data ext4 rw 0 0\n/dev/nvme0n1 /nvme ext4 rw 0 0\n"), 0644); err != nil {
		t.Fatalf("failed to write mock mounts: %v", err)
	}

	c := &DiskIOCollector{
		diskstatsPath: diskstatsPath,
		mountsPath:    mountsPath,
		prevStats:     make(map[string]diskRawIO),
	}

	// First collection establishes snapshots and emits totals, but rates are unavailable.
	events, err := c.Collect(context.Background())
	if err != nil {
		t.Fatalf("first Collect failed: %v", err)
	}
	if len(events) != 3 {
		t.Fatalf("expected 3 first-sample events, got %d", len(events))
	}
	for _, event := range events {
		if event.Data["sample_interval_seconds"] != nil || event.Data["disk_read_bytes_per_second"] != nil {
			t.Fatalf("expected first-sample rate fields to be nil, got %+v", event.Data)
		}
	}

	// Mock second sample after 2 seconds
	sample2 := `   8       0 sda 120 10 240 1200 60 5 120 600 0 220 2200
 259       0 nvme0n1 250 20 500 2500 80 6 160 800 0 350 3500
   7       0 loop0 199 199 199 199 199 199 199 199 199 199 199
 253       0 dm-0 180 15 360 1800 50 4 100 500 0 190 1900
`
	if err := os.WriteFile(diskstatsPath, []byte(sample2), 0644); err != nil {
		t.Fatalf("failed to write mock diskstats sample 2: %v", err)
	}

	// Artificially adjust prevTime to simulate a 2-second interval
	c.prevTime = time.Now().Add(-2 * time.Second)

	events, err = c.Collect(context.Background())
	if err != nil {
		t.Fatalf("second Collect failed: %v", err)
	}

	// We expect sda, nvme0n1, and dm-0 to be reported. loop0 should be ignored.
	if len(events) != 3 {
		t.Fatalf("expected 3 events, got %d", len(events))
	}

	eventsMap := make(map[string]map[string]any)
	for _, ev := range events {
		if ev.Event != "metric_sample" || ev.Collector != "disk_io" {
			t.Errorf("unexpected event headers: %+v", ev)
		}
		dev := ev.Data["device"].(string)
		eventsMap[dev] = ev.Data
	}

	sdaData, ok := eventsMap["sda"]
	if !ok {
		t.Fatal("sda metrics missing")
	}

	if sdaData["disk_reads_completed_total"] != uint64(120) {
		t.Errorf("expected reads total 120, got %v", sdaData["disk_reads_completed_total"])
	}
	if sdaData["resource_id"] != "device:sda" || sdaData["resource_type"] != "device" {
		t.Errorf("expected device resource labels, got %+v", sdaData)
	}
	if sdaData["device_type"] != "disk" || sdaData["parent_device"] != nil {
		t.Errorf("expected sda to be a parent disk, got %+v", sdaData)
	}
	mounts := sdaData["mounts"].([]string)
	if len(mounts) != 1 || mounts[0] != "/data" {
		t.Errorf("expected sda mount mapping, got %+v", mounts)
	}
	if sdaData["sample_interval_seconds"] != 2.0 {
		t.Errorf("expected sample interval 2.0, got %v", sdaData["sample_interval_seconds"])
	}
	if sdaData["disk_read_bytes_total"] != uint64(240*512) {
		t.Errorf("expected read bytes total %v, got %v", 240*512, sdaData["disk_read_bytes_total"])
	}
	if sdaData["disk_read_bytes_per_second"] != 10240.0 {
		t.Errorf("expected read rate 10240, got %v", sdaData["disk_read_bytes_per_second"])
	}
	if sdaData["disk_write_bytes_per_second"] != 5120.0 {
		t.Errorf("expected write rate 5120, got %v", sdaData["disk_write_bytes_per_second"])
	}
	if sdaData["disk_reads_per_second"] != 10.0 {
		t.Errorf("expected reads per second 10.0, got %v", sdaData["disk_reads_per_second"])
	}
	if sdaData["disk_writes_per_second"] != 5.0 {
		t.Errorf("expected writes per second 5.0, got %v", sdaData["disk_writes_per_second"])
	}
	if sdaData["disk_read_latency_seconds_avg"] != 0.01 {
		t.Errorf("expected avg read latency 0.01s, got %v", sdaData["disk_read_latency_seconds_avg"])
	}
	if sdaData["disk_write_latency_seconds_avg"] != 0.01 {
		t.Errorf("expected avg write latency 0.01s, got %v", sdaData["disk_write_latency_seconds_avg"])
	}
	if sdaData["disk_io_util_percent"] != 5.0 {
		t.Errorf("expected util percent 5%%, got %v", sdaData["disk_io_util_percent"])
	}

	if _, ok := eventsMap["loop0"]; ok {
		t.Error("loop0 should have been filtered out")
	}
}

func TestCalculateDiskIORates(t *testing.T) {
	prev := diskRawIO{
		readsCompleted:  10,
		readBytes:       1000,
		readTimeMs:      100,
		writesCompleted: 20,
		writeBytes:      2000,
		writeTimeMs:     200,
		ioTimeMs:        100,
	}
	curr := diskRawIO{
		readsCompleted:  14,
		readBytes:       3000,
		readTimeMs:      180,
		writesCompleted: 22,
		writeBytes:      2600,
		writeTimeMs:     240,
		ioTimeMs:        300,
	}

	rates := calculateDiskIORates(curr, prev, true, 2)

	if !rates.hasSampleInterval || rates.sampleIntervalSeconds != 2 {
		t.Fatalf("expected 2 second sample interval, got %+v", rates)
	}
	if rates.readBytesRate != 1000 {
		t.Errorf("expected read bytes rate 1000, got %v", rates.readBytesRate)
	}
	if rates.writeBytesRate != 300 {
		t.Errorf("expected write bytes rate 300, got %v", rates.writeBytesRate)
	}
	if rates.readsRate != 2 {
		t.Errorf("expected reads rate 2, got %v", rates.readsRate)
	}
	if rates.writesRate != 1 {
		t.Errorf("expected writes rate 1, got %v", rates.writesRate)
	}
	if rates.readLatencyAvg != 0.02 {
		t.Errorf("expected read latency 0.02, got %v", rates.readLatencyAvg)
	}
	if !rates.hasReadLatency || !rates.hasWriteLatency {
		t.Fatalf("expected latency flags, got %+v", rates)
	}
	if rates.writeLatencyAvg != 0.02 {
		t.Errorf("expected write latency 0.02, got %v", rates.writeLatencyAvg)
	}
	if rates.utilPercent != 10 {
		t.Errorf("expected util percent 10, got %v", rates.utilPercent)
	}
}

func TestCalculateDiskIORatesCounterReset(t *testing.T) {
	prev := diskRawIO{
		readsCompleted:  10,
		readBytes:       1000,
		readTimeMs:      100,
		writesCompleted: 20,
		writeBytes:      2000,
		writeTimeMs:     200,
		ioTimeMs:        100,
	}
	curr := diskRawIO{}

	rates := calculateDiskIORates(curr, prev, true, 2)

	if !rates.hasSampleInterval || rates.sampleIntervalSeconds != 2 {
		t.Fatalf("expected sample interval after counter reset, got %+v", rates)
	}
	if rates.readBytesRate != 0 || rates.writeBytesRate != 0 || rates.readsRate != 0 || rates.writesRate != 0 {
		t.Fatalf("expected zero rates after counter reset, got %+v", rates)
	}
	if rates := calculateDiskIORates(prev, diskRawIO{}, false, 2); rates != (diskIORates{}) {
		t.Fatalf("expected zero rates without previous sample, got %+v", rates)
	}
}

func TestParseDiskstatsLine(t *testing.T) {
	device, stat, ok := parseDiskstatsLine("   8       0 sda 100 10 200 1000 50 5 100 500 0 120 1200")
	if !ok {
		t.Fatal("expected physical disk line to parse")
	}
	if device != "sda" {
		t.Fatalf("expected device sda, got %s", device)
	}
	if stat.readsCompleted != 100 || stat.readBytes != 200*512 || stat.readTimeMs != 1000 {
		t.Fatalf("unexpected read stats: %+v", stat)
	}
	if stat.writesCompleted != 50 || stat.writeBytes != 100*512 || stat.writeTimeMs != 500 || stat.ioTimeMs != 120 {
		t.Fatalf("unexpected write stats: %+v", stat)
	}

	if _, _, ok := parseDiskstatsLine("   7       0 loop0 99 99 99 99 99 99 99 99 99 99 99"); ok {
		t.Fatal("expected loop device to be skipped")
	}
	if _, _, ok := parseDiskstatsLine("short line"); ok {
		t.Fatal("expected short line to be skipped")
	}
}
