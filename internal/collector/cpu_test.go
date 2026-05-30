package collector

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestCPUCollector(t *testing.T) {
	tmpDir := t.TempDir()
	statFile := filepath.Join(tmpDir, "stat")
	loadavgFile := filepath.Join(tmpDir, "loadavg")

	err := os.WriteFile(loadavgFile, []byte("0.50 0.40 0.30 1/150 12345\n"), 0644)
	if err != nil {
		t.Fatalf("failed to write mock loadavg: %v", err)
	}

	t.Run("successful delta calculation", func(t *testing.T) {
		prevStates := map[string]cpuRawState{
			"cpu": {
				user:   10,
				system: 10,
				idle:   80,
			},
		}

		c := &CPUCollector{
			procStatPath: statFile,
			loadavgPath:  loadavgFile,
			prevStates:   prevStates,
			hasPrev:      true,
		}

		err := os.WriteFile(statFile, []byte("cpu 40 0 30 130 0 0 0 0 0 0\ncpu0 20 0 15 65 0 0 0 0 0 0\n"), 0644)
		if err != nil {
			t.Fatalf("failed to write mock stat: %v", err)
		}

		events, err := c.Collect(context.Background())
		if err != nil {
			t.Fatalf("Collect failed: %v", err)
		}

		if len(events) != 1 {
			t.Fatalf("expected 1 event, got %d", len(events))
		}

		ev := events[0]
		if ev.Event != "metric_sample" || ev.Collector != "cpu" {
			t.Errorf("unexpected event: %+v", ev)
		}

		val := ev.Data["usage_percent"].(float64)
		if val != 50.0 {
			t.Errorf("expected CPU usage 50.0, got %f", val)
		}

		if ev.Data["load_average_1m"] != 0.50 {
			t.Errorf("expected loadavg 0.50, got %v", ev.Data["load_average_1m"])
		}
	})
}
