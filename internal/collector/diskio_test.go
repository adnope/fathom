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

	// Mock diskstats on sample 1
	sample1 := `   8       0 sda 100 10 200 1000 50 5 100 500 0 120 1200
 259       0 nvme0n1 200 20 400 2000 60 6 120 600 0 150 1500
   7       0 loop0 99 99 99 99 99 99 99 99 99 99 99
 253       0 dm-0 150 15 300 1500 40 4 80 400 0 90 900
`
	if err := os.WriteFile(diskstatsPath, []byte(sample1), 0644); err != nil {
		t.Fatalf("failed to write mock diskstats: %v", err)
	}

	c := &DiskIOCollector{
		diskstatsPath: diskstatsPath,
		prevStats:     make(map[string]diskRawIO),
	}

	// First collection establishes snapshot
	events, err := c.Collect(context.Background())
	if err != nil {
		t.Fatalf("first Collect failed: %v", err)
	}
	if len(events) != 0 {
		t.Fatalf("expected 0 events on first collect, got %d", len(events))
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
	if sdaData["disk_read_bytes_total"] != uint64(240*512) {
		t.Errorf("expected read bytes total %v, got %v", 240*512, sdaData["disk_read_bytes_total"])
	}
	if sdaData["disk_read_bytes_per_second"] != 10240.0 {
		t.Errorf("expected read rate 10240, got %v", sdaData["disk_read_bytes_per_second"])
	}
	if sdaData["disk_write_bytes_per_second"] != 5120.0 {
		t.Errorf("expected write rate 5120, got %v", sdaData["disk_write_bytes_per_second"])
	}
	if sdaData["disk_io_util_percent"] != 5.0 {
		t.Errorf("expected util percent 5%%, got %v", sdaData["disk_io_util_percent"])
	}

	if _, ok := eventsMap["loop0"]; ok {
		t.Error("loop0 should have been filtered out")
	}
}
