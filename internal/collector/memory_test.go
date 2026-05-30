package collector

import (
	"context"
	"os"
	"path/filepath"
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
