package collector

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestSystemCollector(t *testing.T) {
	tmpDir := t.TempDir()

	// Mock uptime
	uptimePath := filepath.Join(tmpDir, "uptime")
	if err := os.WriteFile(uptimePath, []byte("12345.67 98765.43\n"), 0644); err != nil {
		t.Fatalf("failed to write mock uptime: %v", err)
	}

	// Mock stat
	statPath := filepath.Join(tmpDir, "stat")
	statContent := `cpu  100 200 300 400 500 600 700 0 0 0
ctxt 9876543
processes 4567
procs_running 5
procs_blocked 2
`
	if err := os.WriteFile(statPath, []byte(statContent), 0644); err != nil {
		t.Fatalf("failed to write mock stat: %v", err)
	}

	// Mock file-nr (allocated = 10000, unused = 0, max = 1000000, used percent = 1.0%)
	fileNrPath := filepath.Join(tmpDir, "file-nr")
	if err := os.WriteFile(fileNrPath, []byte("10000 0 1000000\n"), 0644); err != nil {
		t.Fatalf("failed to write mock file-nr: %v", err)
	}

	// Mock proc dir for threads
	procDir := filepath.Join(tmpDir, "proc")
	if err := os.MkdirAll(filepath.Join(procDir, "123", "task", "123"), 0755); err != nil {
		t.Fatalf("failed to create mock task dir: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(procDir, "123", "task", "124"), 0755); err != nil {
		t.Fatalf("failed to create mock task dir: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(procDir, "456", "task", "456"), 0755); err != nil {
		t.Fatalf("failed to create mock task dir: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(procDir, "self", "task"), 0755); err != nil {
		t.Fatalf("failed to create mock self task: %v", err)
	}

	c := &SystemCollector{
		procStatPath: statPath,
		uptimePath:   uptimePath,
		fileNrPath:   fileNrPath,
		procDir:      procDir,
	}

	events, err := c.Collect(context.Background())
	if err != nil {
		t.Fatalf("Collect failed: %v", err)
	}

	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}

	ev := events[0]
	if ev.Event != "metric_sample" || ev.Collector != "system" {
		t.Errorf("unexpected event headers: %+v", ev)
	}

	expectedMetrics := map[string]any{
		"system_uptime_seconds":                12345.67,
		"system_processes_running":             uint64(5),
		"system_processes_blocked":             uint64(2),
		"system_processes_total":               uint64(4567),
		"system_context_switches_total":        uint64(9876543),
		"system_file_descriptors_allocated":    uint64(10000),
		"system_file_descriptors_max":          uint64(1000000),
		"system_file_descriptors_used_percent": 1.0,
		"system_threads_total":                 uint64(3),
		"system_context_switches_per_second":   0.0,
		"system_processes_created_per_second":  0.0,
	}

	for k, expectedVal := range expectedMetrics {
		gotVal, ok := ev.Data[k]
		if !ok {
			t.Errorf("missing metric %s", k)
			continue
		}
		if gotVal != expectedVal {
			t.Errorf("for %s: expected %v, got %v", k, expectedVal, gotVal)
		}
	}

	// Update stats to test rate calculations
	statContent2 := `cpu  100 200 300 400 500 600 700 0 0 0
ctxt 9876743
processes 4577
procs_running 6
procs_blocked 1
`
	if err := os.WriteFile(statPath, []byte(statContent2), 0644); err != nil {
		t.Fatalf("failed to write mock stat 2: %v", err)
	}

	// Artificially adjust c.prevTime to simulate 2-second interval
	c.prevTime = time.Now().Add(-2 * time.Second)

	events, err = c.Collect(context.Background())
	if err != nil {
		t.Fatalf("second Collect failed: %v", err)
	}

	ev = events[0]
	// ctxt diff: 9876743 - 9876543 = 200 ctxt / 2s = 100.0 ctxt/sec
	// processes diff: 4577 - 4567 = 10 proc / 2s = 5.0 proc/sec
	if ev.Data["system_context_switches_per_second"] != 100.0 {
		t.Errorf("expected ctxt rate 100.0, got %v", ev.Data["system_context_switches_per_second"])
	}
	if ev.Data["system_processes_created_per_second"] != 5.0 {
		t.Errorf("expected processes rate 5.0, got %v", ev.Data["system_processes_created_per_second"])
	}
}
