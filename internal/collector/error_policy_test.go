package collector

import (
	"bytes"
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCollectorIssueLoggerLogsOnceAndClears(t *testing.T) {
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(prev) })

	issues := newCollectorIssueLogger()
	issues.log(slog.LevelDebug, "cpu", actionOmitMetric, metricFrequencyMHz, "/missing", os.ErrNotExist)
	issues.log(slog.LevelDebug, "cpu", actionOmitMetric, metricFrequencyMHz, "/missing", os.ErrNotExist)

	if got := strings.Count(buf.String(), eventCollectorIssue); got != 1 {
		t.Fatalf("expected one issue log before clear, got %d logs: %s", got, buf.String())
	}

	issues.clear("cpu", actionOmitMetric, metricFrequencyMHz, "/missing")
	issues.log(slog.LevelDebug, "cpu", actionOmitMetric, metricFrequencyMHz, "/missing", os.ErrNotExist)
	if got := strings.Count(buf.String(), eventCollectorIssue); got != 2 {
		t.Fatalf("expected issue to log again after clear, got %d logs: %s", got, buf.String())
	}
}

func TestCPUCollectorOptionalMetricsAreNil(t *testing.T) {
	tmpDir := t.TempDir()
	statFile := filepath.Join(tmpDir, "stat")
	loadavgFile := filepath.Join(tmpDir, "loadavg")
	if err := os.WriteFile(statFile, []byte("cpu 40 0 30 130 0 0 0 0 0 0\ncpu0 20 0 15 65 0 0 0 0 0 0\n"), 0644); err != nil {
		t.Fatalf("failed to write stat: %v", err)
	}
	if err := os.WriteFile(loadavgFile, []byte("0.50 0.40 0.30 1/150 12345\n"), 0644); err != nil {
		t.Fatalf("failed to write loadavg: %v", err)
	}

	c := &CPUCollector{
		procStatPath:    statFile,
		loadavgPath:     loadavgFile,
		procCPUInfoPath: filepath.Join(tmpDir, "missing_cpuinfo"),
		sysCPUPath:      filepath.Join(tmpDir, "sys_cpu"),
		sysHwmonPath:    filepath.Join(tmpDir, "hwmon"),
		sysThermalPath:  filepath.Join(tmpDir, "thermal"),
		powercapPath:    filepath.Join(tmpDir, "powercap"),
		prevStates: map[string]cpuRawState{
			"cpu":  {user: 10, system: 10, idle: 80},
			"cpu0": {user: 5, system: 5, idle: 40},
		},
		hasPrev: true,
	}

	events, err := c.Collect(context.Background())
	if err != nil {
		t.Fatalf("Collect failed: %v", err)
	}
	data := events[0].Data
	for _, key := range []string{"cpu_frequency_mhz_avg", "cpu_frequency_mhz_min", "cpu_frequency_mhz_max", "cpu_temperature_celsius_avg", "cpu_temperature_celsius_max", "cpu_power_watts"} {
		if data[key] != nil {
			t.Fatalf("expected %s to be nil, got %v", key, data[key])
		}
	}
	perCPU := data["per_cpu"].([]PerCPUCore)
	if len(perCPU) != 1 || perCPU[0].FrequencyMHz != nil {
		t.Fatalf("expected per-cpu frequency field to be nil, got %+v", perCPU)
	}
}

func TestRequiredSourceMalformedFailures(t *testing.T) {
	t.Run("cpu skips malformed core", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "stat")
		if err := os.WriteFile(path, []byte("cpu 1 0 0 1 0 0 0 0\ncpu0 bad 0 0 0 0 0 0 0\ncpu1 1 0 0 1 0 0 0 0\n"), 0644); err != nil {
			t.Fatalf("failed to write stat: %v", err)
		}
		states, err := parseProcStat(path)
		if err != nil {
			t.Fatalf("expected malformed per-core line to be skipped: %v", err)
		}
		if _, ok := states["cpu0"]; ok {
			t.Fatal("expected malformed cpu0 line to be skipped")
		}
		if _, ok := states["cpu1"]; !ok {
			t.Fatal("expected valid cpu1 line to be retained")
		}
	})

	t.Run("cpu aggregate", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "stat")
		if err := os.WriteFile(path, []byte("cpu bad 0 0 0 0 0 0 0\ncpu0 1 0 0 1 0 0 0 0\n"), 0644); err != nil {
			t.Fatalf("failed to write stat: %v", err)
		}
		if _, err := parseProcStat(path); err == nil {
			t.Fatal("expected malformed aggregate cpu line to fail")
		}
	})

	t.Run("diskstats all physical malformed", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "diskstats")
		if err := os.WriteFile(path, []byte("8 0 sda bad 0 0 0 0 0 0 0 0 0 0\n"), 0644); err != nil {
			t.Fatalf("failed to write diskstats: %v", err)
		}
		if _, err := parseProcDiskstats(path); err == nil {
			t.Fatal("expected fully malformed physical diskstats to fail")
		}
	})

	t.Run("netdev all interfaces malformed", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "dev")
		content := "Inter-| Receive | Transmit\n face |bytes packets errs drop fifo frame compressed multicast|bytes packets errs drop fifo colls carrier compressed\neth0: bad 1 2 3 0 0 0 0 4 5 6 7 0 0 0 0\n"
		if err := os.WriteFile(path, []byte(content), 0644); err != nil {
			t.Fatalf("failed to write netdev: %v", err)
		}
		if _, err := parseProcNetDev(path); err == nil {
			t.Fatal("expected fully malformed netdev to fail")
		}
	})
}

func TestSystemCollectorOptionalSourcesAreNil(t *testing.T) {
	tmpDir := t.TempDir()
	uptimePath := filepath.Join(tmpDir, "uptime")
	statPath := filepath.Join(tmpDir, "stat")
	if err := os.WriteFile(uptimePath, []byte("1.5 10\n"), 0644); err != nil {
		t.Fatalf("failed to write uptime: %v", err)
	}
	if err := os.WriteFile(statPath, []byte("ctxt 10\nprocesses 2\nprocs_running 1\nprocs_blocked 0\n"), 0644); err != nil {
		t.Fatalf("failed to write stat: %v", err)
	}

	c := &SystemCollector{
		procStatPath: statPath,
		uptimePath:   uptimePath,
		fileNrPath:   filepath.Join(tmpDir, "missing_file_nr"),
		procDir:      filepath.Join(tmpDir, "missing_proc"),
	}
	events, err := c.Collect(context.Background())
	if err != nil {
		t.Fatalf("Collect failed: %v", err)
	}
	data := events[0].Data
	for _, key := range []string{"system_file_descriptors_allocated", "system_file_descriptors_max", "system_file_descriptors_used_percent", "system_threads_total"} {
		if data[key] != nil {
			t.Fatalf("expected %s to be nil, got %v", key, data[key])
		}
	}
}
