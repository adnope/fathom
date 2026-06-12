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
	now           func() time.Time
	prevCtxt      uint64
	prevProcesses uint64
	prevTime      time.Time
	hasPrev       bool
	issues        *collectorIssueLogger
}

type systemSnapshot struct {
	uptime         float64
	processesTotal uint64
	procsRunning   uint64
	procsBlocked   uint64
	ctxt           uint64
	fdsAllocated   uint64
	fdsMax         uint64
	threads        uint64
	hasFileNr      bool
	hasThreads     bool
}

type systemRates struct {
	ctxtRate              float64
	processesRate         float64
	sampleIntervalSeconds float64
	hasSampleInterval     bool
}

// NewSystemCollector constructs a SystemCollector with default paths.
func NewSystemCollector() *SystemCollector {
	return &SystemCollector{
		procStatPath: "/proc/stat",
		uptimePath:   "/proc/uptime",
		fileNrPath:   "/proc/sys/fs/file-nr",
		procDir:      "/proc",
		issues:       newCollectorIssueLogger(),
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
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	snapshot, err := c.collectSystemSnapshot()
	if err != nil {
		return nil, err
	}
	if c.hasPrev && (snapshot.ctxt < c.prevCtxt || snapshot.processesTotal < c.prevProcesses) {
		c.issueLogger().log(slog.LevelDebug, c.Name(), actionZeroMetric, "system_rates", c.procStatPath, nil)
	} else {
		c.issueLogger().clear(c.Name(), actionZeroMetric, "system_rates", c.procStatPath)
	}
	rates := c.updateSystemRates(snapshot.ctxt, snapshot.processesTotal, c.currentTime())

	return []Event{
		{
			Event:     eventMetricSample,
			Collector: c.Name(),
			Component: componentCollector,
			Data:      buildSystemData(snapshot, rates),
		},
	}, nil
}

func (c *SystemCollector) issueLogger() *collectorIssueLogger {
	if c.issues == nil {
		c.issues = newCollectorIssueLogger()
	}
	return c.issues
}

func (c *SystemCollector) collectSystemSnapshot() (systemSnapshot, error) {
	uptime, err := parseUptime(c.uptimePath)
	if err != nil {
		return systemSnapshot{}, fmt.Errorf("read required system source %s: %w", c.uptimePath, err)
	}

	processesTotal, procsRunning, procsBlocked, ctxt, err := parseProcStatFields(c.procStatPath)
	if err != nil {
		return systemSnapshot{}, fmt.Errorf("read required system source %s: %w", c.procStatPath, err)
	}

	fdsAllocated, fdsMax, err := parseFileNrFields(c.fileNrPath)
	hasFileNr := true
	if err != nil {
		hasFileNr = false
		c.issueLogger().log(slog.LevelDebug, c.Name(), actionOmitMetric, "system_file_descriptors", c.fileNrPath, err)
	} else {
		c.issueLogger().clear(c.Name(), actionOmitMetric, "system_file_descriptors", c.fileNrPath)
	}

	threads, err := parseThreadsTotal(c.procDir)
	hasThreads := true
	if err != nil {
		hasThreads = false
		c.issueLogger().log(slog.LevelDebug, c.Name(), actionOmitMetric, "system_threads_total", c.procDir, err)
	} else {
		c.issueLogger().clear(c.Name(), actionOmitMetric, "system_threads_total", c.procDir)
	}

	return systemSnapshot{
		uptime:         uptime,
		processesTotal: processesTotal,
		procsRunning:   procsRunning,
		procsBlocked:   procsBlocked,
		ctxt:           ctxt,
		fdsAllocated:   fdsAllocated,
		fdsMax:         fdsMax,
		threads:        threads,
		hasFileNr:      hasFileNr,
		hasThreads:     hasThreads,
	}, nil
}

func (c *SystemCollector) currentTime() time.Time {
	if c.now != nil {
		return c.now()
	}
	return time.Now()
}

func (c *SystemCollector) updateSystemRates(ctxt, processesTotal uint64, now time.Time) systemRates {
	var rates systemRates

	if !c.hasPrev {
		c.prevCtxt = ctxt
		c.prevProcesses = processesTotal
		c.prevTime = now
		c.hasPrev = true
		return rates
	}

	duration := now.Sub(c.prevTime).Seconds()
	if duration > 0 {
		rates.sampleIntervalSeconds = duration
		rates.hasSampleInterval = true
		if ctxtDiff, ok := counterDiff(ctxt, c.prevCtxt); ok {
			rates.ctxtRate = float64(ctxtDiff) / duration
		}
		if processesDiff, ok := counterDiff(processesTotal, c.prevProcesses); ok {
			rates.processesRate = float64(processesDiff) / duration
		}
	}
	c.prevCtxt = ctxt
	c.prevProcesses = processesTotal
	c.prevTime = now

	return rates
}

func buildSystemData(snapshot systemSnapshot, rates systemRates) map[string]any {
	var fdsAllocated, fdsMax, fdsUsedPercent any
	if snapshot.fdsMax > 0 {
		fdsAllocated = snapshot.fdsAllocated
		fdsMax = snapshot.fdsMax
		fdsUsedPercent = round((float64(snapshot.fdsAllocated)/float64(snapshot.fdsMax))*100, 2)
	} else if snapshot.hasFileNr {
		fdsAllocated = snapshot.fdsAllocated
		fdsMax = snapshot.fdsMax
		fdsUsedPercent = 0.0
	}

	var threads any
	if snapshot.hasThreads {
		threads = snapshot.threads
	}

	return map[string]any{
		"sample_interval_seconds":              optionalRoundedFloat(rates.hasSampleInterval, rates.sampleIntervalSeconds, 3),
		"system_uptime_seconds":                round(snapshot.uptime, 2),
		"system_processes_running":             snapshot.procsRunning,
		"system_processes_blocked":             snapshot.procsBlocked,
		"system_processes_total":               snapshot.processesTotal,
		"system_context_switches_total":        snapshot.ctxt,
		"system_file_descriptors_allocated":    fdsAllocated,
		"system_file_descriptors_max":          fdsMax,
		"system_file_descriptors_used_percent": fdsUsedPercent,
		"system_threads_total":                 threads,
		"system_context_switches_per_second":   optionalRoundedFloat(rates.hasSampleInterval, rates.ctxtRate, 2),
		"system_processes_created_per_second":  optionalRoundedFloat(rates.hasSampleInterval, rates.processesRate, 2),
	}
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
	var hasProcesses, hasProcsRunning, hasProcsBlocked, hasCtxt bool
	for scanner.Scan() {
		line := scanner.Text()
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		switch fields[0] {
		case "processes":
			processesTotal, err = strconv.ParseUint(fields[1], 10, 64)
			hasProcesses = err == nil
		case "procs_running":
			procsRunning, err = strconv.ParseUint(fields[1], 10, 64)
			hasProcsRunning = err == nil
		case "procs_blocked":
			procsBlocked, err = strconv.ParseUint(fields[1], 10, 64)
			hasProcsBlocked = err == nil
		case "ctxt":
			ctxt, err = strconv.ParseUint(fields[1], 10, 64)
			hasCtxt = err == nil
		}
		if err != nil {
			return 0, 0, 0, 0, err
		}
	}
	if err := scanner.Err(); err != nil {
		return 0, 0, 0, 0, err
	}
	if !hasProcesses || !hasProcsRunning || !hasProcsBlocked || !hasCtxt {
		return 0, 0, 0, 0, fmt.Errorf("missing required proc stat fields")
	}
	return processesTotal, procsRunning, procsBlocked, ctxt, nil
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
