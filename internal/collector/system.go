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
)

// SystemCollector tracks overall system stats.
type SystemCollector struct {
	mu           sync.Mutex
	procStatPath string
	uptimePath   string
	fileNrPath   string
	procDir      string
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

	fds, err := parseFileNr(c.fileNrPath)
	if err != nil {
		slog.Debug("failed to parse file descriptor count", slog.String("error", err.Error()))
	}

	threads, err := parseThreadsTotal(c.procDir)
	if err != nil {
		slog.Debug("failed to count total threads", slog.String("error", err.Error()))
	}

	// Build the event
	data := map[string]any{
		"system_uptime_seconds":             round(uptime, 2),
		"system_processes_running":          procsRunning,
		"system_processes_blocked":          procsBlocked,
		"system_processes_total":            processesTotal,
		"system_context_switches_total":     ctxt,
		"system_file_descriptors_allocated": fds,
		"system_threads_total":              threads,
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

func parseFileNr(path string) (uint64, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}
	fields := strings.Fields(string(data))
	if len(fields) < 1 {
		return 0, fmt.Errorf("empty file-nr")
	}
	return strconv.ParseUint(fields[0], 10, 64)
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
