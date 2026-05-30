package collector

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
)

// MemoryCollector monitors detailed memory and swap utilization.
type MemoryCollector struct {
	path string
}

// NewMemoryCollector constructs a MemoryCollector monitoring /proc/meminfo.
func NewMemoryCollector() *MemoryCollector {
	return &MemoryCollector{
		path: "/proc/meminfo",
	}
}

// Name returns the name of the collector.
func (c *MemoryCollector) Name() string {
	return "memory"
}

// Collect reads meminfo and returns Memory and Swap stats.
func (c *MemoryCollector) Collect(ctx context.Context) ([]Event, error) {
	file, err := os.Open(c.path)
	if err != nil {
		return nil, fmt.Errorf("failed to open meminfo file: %w", err)
	}
	defer file.Close()

	var (
		total        uint64
		free         uint64
		available    uint64
		buffers      uint64
		cached       uint64
		swapTotal    uint64
		swapFree     uint64
		hasAvailable bool
	)

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		key := strings.TrimSuffix(fields[0], ":")
		val, err := strconv.ParseUint(fields[1], 10, 64)
		if err != nil {
			continue
		}

		valBytes := val * 1024 // Convert kB to Bytes

		switch key {
		case "MemTotal":
			total = valBytes
		case "MemFree":
			free = valBytes
		case "MemAvailable":
			available = valBytes
			hasAvailable = true
		case "Buffers":
			buffers = valBytes
		case "Cached":
			cached = valBytes
		case "SwapTotal":
			swapTotal = valBytes
		case "SwapFree":
			swapFree = valBytes
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("failed to scan meminfo: %w", err)
	}

	if total == 0 {
		return nil, fmt.Errorf("missing MemTotal in meminfo")
	}

	if !hasAvailable {
		available = free + buffers + cached
	}

	var used uint64
	if total > available {
		used = total - available
	}

	usedPercent := round((float64(used)/float64(total))*100, 2)
	availablePercent := round((float64(available)/float64(total))*100, 2)
	freePercent := round((float64(free)/float64(total))*100, 2)
	cachedPercent := round((float64(cached)/float64(total))*100, 2)

	swapUsed := swapTotal - swapFree
	var swapUsedPercent float64
	var swapFreePercent float64
	if swapTotal > 0 {
		swapUsedPercent = round((float64(swapUsed)/float64(swapTotal))*100, 2)
		swapFreePercent = round((float64(swapFree)/float64(swapTotal))*100, 2)
	}

	data := map[string]any{
		"memory_total_bytes":       total,
		"memory_used_bytes":        used,
		"memory_available_bytes":   available,
		"memory_free_bytes":        free,
		"memory_cached_bytes":      cached,
		"memory_buffers_bytes":     buffers,
		"memory_used_mib":          round(float64(used)/(1024*1024), 2),
		"memory_used_gib":          round(float64(used)/(1024*1024*1024), 2),
		"memory_available_mib":     round(float64(available)/(1024*1024), 2),
		"memory_available_gib":     round(float64(available)/(1024*1024*1024), 2),
		"memory_used_percent":      usedPercent,
		"memory_available_percent": availablePercent,
		"memory_free_percent":      freePercent,
		"memory_cached_percent":    cachedPercent,
		"swap_total_bytes":         swapTotal,
		"swap_used_bytes":          swapUsed,
		"swap_free_bytes":          swapFree,
		"swap_used_mib":            round(float64(swapUsed)/(1024*1024), 2),
		"swap_free_mib":            round(float64(swapFree)/(1024*1024), 2),
		"swap_used_percent":        swapUsedPercent,
		"swap_free_percent":        swapFreePercent,
	}

	return []Event{
		{
			Event:     "metric_sample",
			Collector: "memory",
			Component: "collector",
			Data:      data,
		},
	}, nil
}
