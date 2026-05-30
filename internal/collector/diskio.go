package collector

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type diskRawIO struct {
	readsCompleted  uint64
	readBytes       uint64
	readTimeMs      uint64
	writesCompleted uint64
	writeBytes      uint64
	writeTimeMs     uint64
	ioTimeMs        uint64
}

// DiskIOCollector monitors disk read/write statistics and utilization.
type DiskIOCollector struct {
	mu            sync.Mutex
	diskstatsPath string
	prevStats     map[string]diskRawIO
	prevTime      time.Time
	hasPrev       bool
}

// NewDiskIOCollector constructs a DiskIOCollector with default path.
func NewDiskIOCollector() *DiskIOCollector {
	return &DiskIOCollector{
		diskstatsPath: "/proc/diskstats",
		prevStats:     make(map[string]diskRawIO),
	}
}

// Name returns the collector name.
func (c *DiskIOCollector) Name() string {
	return "disk_io"
}

// Collect returns disk IO stats and rates since last sample.
func (c *DiskIOCollector) Collect(ctx context.Context) ([]Event, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now()
	currentStats, err := parseProcDiskstats(c.diskstatsPath)
	if err != nil {
		return nil, fmt.Errorf("failed to parse proc diskstats: %w", err)
	}

	var events []Event

	if !c.hasPrev {
		c.prevStats = currentStats
		c.prevTime = now
		c.hasPrev = true
		return nil, nil
	}

	duration := now.Sub(c.prevTime).Seconds()
	durationMs := duration * 1000.0

	var devices []string
	for dev := range currentStats {
		devices = append(devices, dev)
	}
	sort.Strings(devices)

	for _, dev := range devices {
		curr := currentStats[dev]
		prev, ok := c.prevStats[dev]

		var readBytesRate, writeBytesRate, utilPercent float64
		var readsRate, writesRate, readLatencyAvg, writeLatencyAvg float64

		if ok && duration > 0 {
			if curr.readBytes >= prev.readBytes {
				readBytesRate = float64(curr.readBytes-prev.readBytes) / duration
			}
			if curr.writeBytes >= prev.writeBytes {
				writeBytesRate = float64(curr.writeBytes-prev.writeBytes) / duration
			}
			if curr.readsCompleted >= prev.readsCompleted {
				readsRate = float64(curr.readsCompleted-prev.readsCompleted) / duration

				readsDiff := curr.readsCompleted - prev.readsCompleted
				if readsDiff > 0 && curr.readTimeMs >= prev.readTimeMs {
					readLatencyAvg = (float64(curr.readTimeMs-prev.readTimeMs) / 1000.0) / float64(readsDiff)
				}
			}
			if curr.writesCompleted >= prev.writesCompleted {
				writesRate = float64(curr.writesCompleted-prev.writesCompleted) / duration

				writesDiff := curr.writesCompleted - prev.writesCompleted
				if writesDiff > 0 && curr.writeTimeMs >= prev.writeTimeMs {
					writeLatencyAvg = (float64(curr.writeTimeMs-prev.writeTimeMs) / 1000.0) / float64(writesDiff)
				}
			}
			if curr.ioTimeMs >= prev.ioTimeMs {
				ioTimeDiff := curr.ioTimeMs - prev.ioTimeMs
				utilPercent = (float64(ioTimeDiff) / durationMs) * 100.0
				if utilPercent > 100.0 {
					utilPercent = 100.0
				}
			}
		}

		events = append(events, Event{
			Event:     "metric_sample",
			Collector: "disk_io",
			Component: "collector",
			Data: map[string]any{
				"device":                         dev,
				"disk_read_bytes_total":          curr.readBytes,
				"disk_write_bytes_total":         curr.writeBytes,
				"disk_reads_completed_total":     curr.readsCompleted,
				"disk_writes_completed_total":    curr.writesCompleted,
				"disk_read_bytes_per_second":     round(readBytesRate, 2),
				"disk_write_bytes_per_second":    round(writeBytesRate, 2),
				"disk_reads_per_second":          round(readsRate, 2),
				"disk_writes_per_second":         round(writesRate, 2),
				"disk_read_latency_seconds_avg":  round(readLatencyAvg, 6),
				"disk_write_latency_seconds_avg": round(writeLatencyAvg, 6),
				"disk_io_time_seconds_total":     round(float64(curr.ioTimeMs)/1000.0, 3),
				"disk_io_util_percent":           round(utilPercent, 2),
			},
		})
	}

	c.prevStats = currentStats
	c.prevTime = now

	return events, nil
}

func parseProcDiskstats(path string) (map[string]diskRawIO, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	res := make(map[string]diskRawIO)
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		fields := strings.Fields(line)
		if len(fields) < 14 {
			continue
		}

		dev := fields[2]
		if !isPhysicalDisk(dev) {
			continue
		}

		readsCompleted, _ := strconv.ParseUint(fields[3], 10, 64)
		sectorsRead, _ := strconv.ParseUint(fields[5], 10, 64)
		readTimeMs, _ := strconv.ParseUint(fields[6], 10, 64)
		writesCompleted, _ := strconv.ParseUint(fields[7], 10, 64)
		sectorsWritten, _ := strconv.ParseUint(fields[9], 10, 64)
		writeTimeMs, _ := strconv.ParseUint(fields[10], 10, 64)
		ioTimeMs, _ := strconv.ParseUint(fields[12], 10, 64)

		res[dev] = diskRawIO{
			readsCompleted:  readsCompleted,
			readBytes:       sectorsRead * 512,
			readTimeMs:      readTimeMs,
			writesCompleted: writesCompleted,
			writeBytes:      sectorsWritten * 512,
			writeTimeMs:     writeTimeMs,
			ioTimeMs:        ioTimeMs,
		}
	}
	return res, scanner.Err()
}

func isPhysicalDisk(device string) bool {
	if strings.HasPrefix(device, "sd") ||
		strings.HasPrefix(device, "nvme") ||
		strings.HasPrefix(device, "vd") ||
		strings.HasPrefix(device, "hd") ||
		strings.HasPrefix(device, "dm-") {
		return true
	}
	return false
}
