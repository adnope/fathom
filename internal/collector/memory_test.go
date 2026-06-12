package collector

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestMemoryCollector(t *testing.T) {
	tmpDir := t.TempDir()
	memFile := filepath.Join(tmpDir, "meminfo")

	t.Run("meminfo with MemAvailable and commits", func(t *testing.T) {
		content := `MemTotal:       10000000 kB
MemFree:         1000000 kB
MemAvailable:    4000000 kB
Buffers:          500000 kB
Cached:          2000000 kB
SwapTotal:       4000000 kB
SwapFree:        3000000 kB
Dirty:             100000 kB
Writeback:          50000 kB
Slab:              200000 kB
CommitLimit:       800000 kB
Committed_AS:      400000 kB
`
		err := os.WriteFile(memFile, []byte(content), 0644)
		if err != nil {
			t.Fatalf("failed to write mock meminfo: %v", err)
		}

		c := &MemoryCollector{path: memFile}
		events, err := c.Collect(context.Background())
		if err != nil {
			t.Fatalf("Collect failed: %v", err)
		}

		if len(events) != 1 {
			t.Fatalf("expected 1 event, got %d", len(events))
		}

		ev := events[0]
		if ev.Event != "metric_sample" || ev.Collector != "memory" {
			t.Errorf("unexpected event: %+v", ev)
		}

		if ev.Data["memory_used_percent"] != 60.0 {
			t.Errorf("expected 60%% used, got %v", ev.Data["memory_used_percent"])
		}

		if ev.Data["swap_used_percent"] != 25.0 {
			t.Errorf("expected swap 25%% used, got %v", ev.Data["swap_used_percent"])
		}

		if ev.Data["memory_dirty_bytes"] != uint64(100000*1024) {
			t.Errorf("expected dirty bytes 102400000, got %v", ev.Data["memory_dirty_bytes"])
		}

		if ev.Data["memory_writeback_bytes"] != uint64(50000*1024) {
			t.Errorf("expected writeback bytes 51200000, got %v", ev.Data["memory_writeback_bytes"])
		}

		if ev.Data["memory_slab_bytes"] != uint64(200000*1024) {
			t.Errorf("expected slab bytes 204800000, got %v", ev.Data["memory_slab_bytes"])
		}

		if ev.Data["memory_committed_as_bytes"] != uint64(400000*1024) {
			t.Errorf("expected committed_as bytes 409600000, got %v", ev.Data["memory_committed_as_bytes"])
		}

		if ev.Data["memory_commit_limit_bytes"] != uint64(800000*1024) {
			t.Errorf("expected commit_limit bytes 819200000, got %v", ev.Data["memory_commit_limit_bytes"])
		}

		if ev.Data["memory_commit_percent"] != 50.0 {
			t.Errorf("expected commit percent 50.0, got %v", ev.Data["memory_commit_percent"])
		}
	})
}

func TestParseMemInfo(t *testing.T) {
	stats, err := parseMemInfo(strings.NewReader(`MemTotal: 1000 kB
MemFree: bad kB
MemAvailable: 700 kB
Cached: 300 kB
`))
	if err != nil {
		t.Fatalf("parseMemInfo failed: %v", err)
	}

	if stats.total != 1000*1024 {
		t.Fatalf("expected total bytes %d, got %d", 1000*1024, stats.total)
	}
	if stats.free != 0 {
		t.Fatalf("expected malformed MemFree to be ignored, got %d", stats.free)
	}
	if stats.available != 700*1024 || !stats.hasAvailable {
		t.Fatalf("expected available bytes with flag, got %d flag=%v", stats.available, stats.hasAvailable)
	}
}

func TestMemoryCollectorMemAvailableFallback(t *testing.T) {
	memFile := filepath.Join(t.TempDir(), "meminfo")
	content := `MemTotal:       1000 kB
MemFree:         100 kB
Buffers:         200 kB
Cached:          300 kB
`
	if err := os.WriteFile(memFile, []byte(content), 0644); err != nil {
		t.Fatalf("failed to write mock meminfo: %v", err)
	}

	c := &MemoryCollector{path: memFile}
	events, err := c.Collect(context.Background())
	if err != nil {
		t.Fatalf("Collect failed: %v", err)
	}

	data := events[0].Data
	if data["memory_available_bytes"] != uint64(600*1024) {
		t.Fatalf("expected fallback available bytes %d, got %v", 600*1024, data["memory_available_bytes"])
	}
	if data["memory_used_percent"] != 40.0 {
		t.Fatalf("expected used percent 40.0, got %v", data["memory_used_percent"])
	}
}

func TestMemoryCollectorMissingMemTotal(t *testing.T) {
	memFile := filepath.Join(t.TempDir(), "meminfo")
	if err := os.WriteFile(memFile, []byte("MemFree: 100 kB\n"), 0644); err != nil {
		t.Fatalf("failed to write mock meminfo: %v", err)
	}

	c := &MemoryCollector{path: memFile}
	if _, err := c.Collect(context.Background()); err == nil {
		t.Fatal("expected missing MemTotal error")
	}
}
