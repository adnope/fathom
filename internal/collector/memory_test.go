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

	t.Run("meminfo with MemAvailable", func(t *testing.T) {
		content := `MemTotal:       10000000 kB
MemFree:         1000000 kB
MemAvailable:    4000000 kB
Buffers:          500000 kB
Cached:          2000000 kB
SwapTotal:       4000000 kB
SwapFree:        3000000 kB
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
	})
}
