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

		val := ev.Data["cpu_usage_percent"].(float64)
		if val != 50.0 {
			t.Errorf("expected CPU usage 50.0, got %f", val)
		}

		if ev.Data["cpu_load_average_1m"] != 0.50 {
			t.Errorf("expected loadavg 0.50, got %v", ev.Data["cpu_load_average_1m"])
		}

		if _, ok := ev.Data["sample_interval_seconds"]; !ok {
			t.Fatal("expected sample_interval_seconds key")
		}
	})
}

func TestCalculateCPUUsage(t *testing.T) {
	prev := cpuRawState{
		user:   10,
		system: 10,
		idle:   80,
	}
	cur := cpuRawState{
		user:   40,
		system: 30,
		idle:   130,
	}

	usage := calculateCPUUsage(cur, prev)

	if usage.usagePercent != 50.0 {
		t.Errorf("expected usage 50.0, got %v", usage.usagePercent)
	}
	if usage.userPercent != 30.0 {
		t.Errorf("expected user 30.0, got %v", usage.userPercent)
	}
	if usage.sysPercent != 20.0 {
		t.Errorf("expected system 20.0, got %v", usage.sysPercent)
	}
	if usage.idlePercent != 50.0 {
		t.Errorf("expected idle 50.0, got %v", usage.idlePercent)
	}
	if usage.iowaitPercent != 0.0 {
		t.Errorf("expected iowait 0.0, got %v", usage.iowaitPercent)
	}
	if usage.stealPercent != 0.0 {
		t.Errorf("expected steal 0.0, got %v", usage.stealPercent)
	}
}

func TestBuildPerCPUMetricsSortsAndRoundsFrequencies(t *testing.T) {
	currentStates := map[string]cpuRawState{
		"cpu":  {user: 100, idle: 100},
		"cpu2": {user: 30, idle: 70},
		"cpu0": {user: 20, idle: 80},
	}
	prevStates := map[string]cpuRawState{
		"cpu2": {user: 10, idle: 40},
		"cpu0": {user: 10, idle: 40},
	}
	freqs := map[string]float64{
		"cpu2": 2200.129,
	}

	metrics := buildPerCPUMetrics(currentStates, prevStates, freqs)

	if len(metrics) != 2 {
		t.Fatalf("expected 2 per-CPU metrics, got %d", len(metrics))
	}
	if metrics[0].CPU != "cpu0" || metrics[1].CPU != "cpu2" {
		t.Fatalf("expected CPU metrics sorted by index, got %+v", metrics)
	}
	if metrics[0].UsagePercent != 20.0 {
		t.Errorf("expected cpu0 usage 20.0, got %v", metrics[0].UsagePercent)
	}
	if metrics[0].FrequencyMHz != nil {
		t.Errorf("expected cpu0 frequency to be omitted, got %v", *metrics[0].FrequencyMHz)
	}
	if metrics[1].UsagePercent != 40.0 {
		t.Errorf("expected cpu2 usage 40.0, got %v", metrics[1].UsagePercent)
	}
	if metrics[1].FrequencyMHz == nil || *metrics[1].FrequencyMHz != 2200.13 {
		t.Fatalf("expected cpu2 frequency 2200.13, got %v", metrics[1].FrequencyMHz)
	}
}

func TestSummarizeCPUFrequencies(t *testing.T) {
	avg, min, max, ok := summarizeCPUFrequencies(map[string]float64{
		"cpu0": 1000.125,
		"cpu1": 2000.125,
	})

	if !ok {
		t.Fatal("expected frequency summary to be available")
	}
	if avg != 1500.13 || min != 1000.13 || max != 2000.13 {
		t.Fatalf("unexpected frequency summary: avg=%v min=%v max=%v", avg, min, max)
	}

	avg, min, max, ok = summarizeCPUFrequencies(nil)
	if ok || avg != 0 || min != 0 || max != 0 {
		t.Fatalf("expected empty summary for missing frequencies, got avg=%v min=%v max=%v ok=%v", avg, min, max, ok)
	}
}
