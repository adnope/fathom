package collector

import (
	"bufio"
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

// SystemCollector tracks overall system stats.
type SystemCollector struct {
	mu            sync.Mutex
	procStatPath  string
	uptimePath    string
	fileNrPath    string
	procDir       string
	prevCtxt      uint64
	prevProcesses uint64
	prevTime      time.Time
	hasPrev       bool
}

// NewSystemCollector constructs a SystemCollector with default paths.
func NewSystemCollector() *SystemCollector {
	return &SystemCollector{
		procStatPath: "/proc/stat",
		uptimePath:   "/proc/uptime",
		fileNrPath:   "/proc/sys/fs/file-nr",
		procDir:      "/proc",
	}
}

// Name returns the name of the collector.
func (c *SystemCollector) Name() string {
	return "system"
}

// Collect gathers system metrics.
func (c *SystemCollector) Collect(ctx context.Context) ([]Event, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	uptime, err := parseUptime(c.uptimePath)
	if err != nil {
		slog.Debug("failed to parse uptime", slog.String("error", err.Error()))
	}

	processesTotal, procsRunning, procsBlocked, ctxt, err := parseProcStatFields(c.procStatPath)
	if err != nil {
		slog.Debug("failed to parse proc stat fields", slog.String("error", err.Error()))
	}

	fdsAllocated, fdsMax, err := parseFileNrFields(c.fileNrPath)
	if err != nil {
		slog.Debug("failed to parse file descriptor fields", slog.String("error", err.Error()))
	}

	threads, err := parseThreadsTotal(c.procDir)
	if err != nil {
		slog.Debug("failed to count total threads", slog.String("error", err.Error()))
	}

	var fdsUsedPercent float64
	if fdsMax > 0 {
		fdsUsedPercent = round((float64(fdsAllocated)/float64(fdsMax))*100, 2)
	}

	var ctxtRate, processesRate float64
	now := time.Now()

	if !c.hasPrev {
		c.prevCtxt = ctxt
		c.prevProcesses = processesTotal
		c.prevTime = now
		c.hasPrev = true
	} else {
		duration := now.Sub(c.prevTime).Seconds()
		if duration > 0 {
			if ctxt >= c.prevCtxt {
				ctxtRate = float64(ctxt-c.prevCtxt) / duration
			}
			if processesTotal >= c.prevProcesses {
				processesRate = float64(processesTotal-c.prevProcesses) / duration
			}
		}
		c.prevCtxt = ctxt
		c.prevProcesses = processesTotal
		c.prevTime = now
	}

	// Build the event
	data := map[string]any{
		"system_uptime_seconds":                round(uptime, 2),
		"system_processes_running":             procsRunning,
		"system_processes_blocked":             procsBlocked,
		"system_processes_total":               processesTotal,
		"system_context_switches_total":        ctxt,
		"system_file_descriptors_allocated":    fdsAllocated,
		"system_file_descriptors_max":          fdsMax,
		"system_file_descriptors_used_percent": fdsUsedPercent,
		"system_threads_total":                 threads,
		"system_context_switches_per_second":   round(ctxtRate, 2),
		"system_processes_created_per_second":  round(processesRate, 2),
	}

	return []Event{
		{
			Event:     "metric_sample",
			Collector: "system",
			Component: "collector",
			Data:      data,
		},
	}, nil
}

func parseUptime(path string) (float64, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}
	fields := strings.Fields(string(data))
	if len(fields) < 1 {
		return 0, fmt.Errorf("empty uptime file")
	}
	return strconv.ParseFloat(fields[0], 64)
}

func parseProcStatFields(path string) (processesTotal, procsRunning, procsBlocked, ctxt uint64, err error) {
	file, err := os.Open(path)
	if err != nil {
		return 0, 0, 0, 0, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		switch fields[0] {
		case "processes":
			processesTotal, _ = strconv.ParseUint(fields[1], 10, 64)
		case "procs_running":
			procsRunning, _ = strconv.ParseUint(fields[1], 10, 64)
		case "procs_blocked":
			procsBlocked, _ = strconv.ParseUint(fields[1], 10, 64)
		case "ctxt":
			ctxt, _ = strconv.ParseUint(fields[1], 10, 64)
		}
	}
	return processesTotal, procsRunning, procsBlocked, ctxt, scanner.Err()
}

func parseFileNrFields(path string) (allocated, max uint64, err error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, 0, err
	}
	fields := strings.Fields(string(data))
	if len(fields) < 3 {
		return 0, 0, fmt.Errorf("insufficient fields in file-nr")
	}
	allocated, err = strconv.ParseUint(fields[0], 10, 64)
	if err != nil {
		return 0, 0, err
	}
	max, err = strconv.ParseUint(fields[2], 10, 64)
	if err != nil {
		return 0, 0, err
	}
	return allocated, max, nil
}

func parseThreadsTotal(procDir string) (uint64, error) {
	entries, err := os.ReadDir(procDir)
	if err != nil {
		return 0, err
	}
	var totalThreads uint64
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		name := entry.Name()
		if name[0] < '0' || name[0] > '9' {
			continue
		}
		if _, err := strconv.Atoi(name); err != nil {
			continue
		}
		taskEntries, err := os.ReadDir(filepath.Join(procDir, name, "task"))
		if err != nil {
			continue
		}
		totalThreads += uint64(len(taskEntries))
	}
	return totalThreads, nil
}
