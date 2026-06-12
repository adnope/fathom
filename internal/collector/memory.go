package collector

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
)

type memoryStats struct {
	total        uint64
	free         uint64
	available    uint64
	buffers      uint64
	cached       uint64
	swapTotal    uint64
	swapFree     uint64
	dirty        uint64
	writeback    uint64
	slab         uint64
	committedAs  uint64
	commitLimit  uint64
	hasAvailable bool
}

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
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	file, err := os.Open(c.path)
	if err != nil {
		return nil, fmt.Errorf("read required memory source %s: %w", c.path, err)
	}
	defer file.Close()

	stats, err := parseMemInfo(file)
	if err != nil {
		return nil, fmt.Errorf("scan required memory source %s: %w", c.path, err)
	}
	if stats.total == 0 {
		return nil, fmt.Errorf("required memory source %s missing MemTotal", c.path)
	}
	if !stats.hasAvailable {
		stats.available = stats.free + stats.buffers + stats.cached
	}

	return []Event{
		{
			Event:     eventMetricSample,
			Collector: c.Name(),
			Component: componentCollector,
			Data:      buildMemoryData(stats),
		},
	}, nil
}

func parseMemInfo(reader io.Reader) (memoryStats, error) {
	var stats memoryStats
	scanner := bufio.NewScanner(reader)
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
			stats.total = valBytes
		case "MemFree":
			stats.free = valBytes
		case "MemAvailable":
			stats.available = valBytes
			stats.hasAvailable = true
		case "Buffers":
			stats.buffers = valBytes
		case "Cached":
			stats.cached = valBytes
		case "SwapTotal":
			stats.swapTotal = valBytes
		case "SwapFree":
			stats.swapFree = valBytes
		case "Dirty":
			stats.dirty = valBytes
		case "Writeback":
			stats.writeback = valBytes
		case "Slab":
			stats.slab = valBytes
		case "Committed_AS":
			stats.committedAs = valBytes
		case "CommitLimit":
			stats.commitLimit = valBytes
		}
	}
	if err := scanner.Err(); err != nil {
		return memoryStats{}, err
	}
	return stats, nil
}

func buildMemoryData(stats memoryStats) map[string]any {
	var used uint64
	if stats.total > stats.available {
		used = stats.total - stats.available
	}

	usedPercent := round((float64(used)/float64(stats.total))*100, 2)
	availablePercent := round((float64(stats.available)/float64(stats.total))*100, 2)
	freePercent := round((float64(stats.free)/float64(stats.total))*100, 2)
	cachedPercent := round((float64(stats.cached)/float64(stats.total))*100, 2)

	var swapUsed uint64
	if stats.swapTotal > stats.swapFree {
		swapUsed = stats.swapTotal - stats.swapFree
	}
	var swapUsedPercent float64
	var swapFreePercent float64
	if stats.swapTotal > 0 {
		swapUsedPercent = round((float64(swapUsed)/float64(stats.swapTotal))*100, 2)
		swapFreePercent = round((float64(stats.swapFree)/float64(stats.swapTotal))*100, 2)
	}

	var commitPercent float64
	if stats.commitLimit > 0 {
		commitPercent = round((float64(stats.committedAs)/float64(stats.commitLimit))*100, 2)
	}

	return map[string]any{
		"memory_total_bytes":        stats.total,
		"memory_used_bytes":         used,
		"memory_available_bytes":    stats.available,
		"memory_free_bytes":         stats.free,
		"memory_cached_bytes":       stats.cached,
		"memory_buffers_bytes":      stats.buffers,
		"memory_used_percent":       usedPercent,
		"memory_available_percent":  availablePercent,
		"memory_free_percent":       freePercent,
		"memory_cached_percent":     cachedPercent,
		"swap_total_bytes":          stats.swapTotal,
		"swap_used_bytes":           swapUsed,
		"swap_free_bytes":           stats.swapFree,
		"swap_used_percent":         swapUsedPercent,
		"swap_free_percent":         swapFreePercent,
		"memory_dirty_bytes":        stats.dirty,
		"memory_writeback_bytes":    stats.writeback,
		"memory_slab_bytes":         stats.slab,
		"memory_committed_as_bytes": stats.committedAs,
		"memory_commit_limit_bytes": stats.commitLimit,
		"memory_commit_percent":     commitPercent,
	}
}
