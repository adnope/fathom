package collector

import (
	"bufio"
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
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

type diskIORates struct {
	readBytesRate         float64
	writeBytesRate        float64
	readsRate             float64
	writesRate            float64
	readLatencyAvg        float64
	writeLatencyAvg       float64
	utilPercent           float64
	sampleIntervalSeconds float64
	hasSampleInterval     bool
	hasReadLatency        bool
	hasWriteLatency       bool
}

// DiskIOCollector monitors disk read/write statistics and utilization.
type DiskIOCollector struct {
	mu            sync.Mutex
	diskstatsPath string
	mountsPath    string
	prevStats     map[string]diskRawIO
	prevTime      time.Time
	hasPrev       bool
	issues        *collectorIssueLogger
}

// NewDiskIOCollector constructs a DiskIOCollector with default path.
func NewDiskIOCollector() *DiskIOCollector {
	return &DiskIOCollector{
		diskstatsPath: "/proc/diskstats",
		mountsPath:    "/proc/mounts",
		prevStats:     make(map[string]diskRawIO),
		issues:        newCollectorIssueLogger(),
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
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	now := time.Now()
	currentStats, err := parseProcDiskstats(c.diskstatsPath)
	if err != nil {
		return nil, fmt.Errorf("read required disk io source %s: %w", c.diskstatsPath, err)
	}

	mountsByDevice, hasMountMappings := c.collectMountMappings()

	var events []Event
	if !c.hasPrev {
		for _, dev := range sortedDiskDevices(currentStats) {
			events = append(events, buildDiskIOEvent(dev, currentStats[dev], diskIORates{}, diskIOMountsForDevice(dev, mountsByDevice, hasMountMappings)))
		}
		c.prevStats = currentStats
		c.prevTime = now
		c.hasPrev = true
		return events, nil
	}

	duration := now.Sub(c.prevTime).Seconds()

	for _, dev := range sortedDiskDevices(currentStats) {
		curr := currentStats[dev]
		prev, ok := c.prevStats[dev]
		if ok && diskIOCounterReset(curr, prev) {
			c.issueLogger().log(slog.LevelDebug, c.Name(), actionZeroMetric, "disk_io_rates", c.diskstatsPath, nil,
				slog.String("resource_type", resourceTypeDevice),
				slog.String("resource", dev),
			)
		} else {
			c.issueLogger().clear(c.Name(), actionZeroMetric, "disk_io_rates", c.diskstatsPath,
				slog.String("resource_type", resourceTypeDevice),
				slog.String("resource", dev),
			)
		}
		rates := calculateDiskIORates(curr, prev, ok, duration)
		events = append(events, buildDiskIOEvent(dev, curr, rates, diskIOMountsForDevice(dev, mountsByDevice, hasMountMappings)))
	}

	c.prevStats = currentStats
	c.prevTime = now

	return events, nil
}

func (c *DiskIOCollector) issueLogger() *collectorIssueLogger {
	if c.issues == nil {
		c.issues = newCollectorIssueLogger()
	}
	return c.issues
}

func sortedDiskDevices(stats map[string]diskRawIO) []string {
	var devices []string
	for device := range stats {
		devices = append(devices, device)
	}
	sort.Strings(devices)
	return devices
}

func calculateDiskIORates(curr, prev diskRawIO, hasPrev bool, duration float64) diskIORates {
	if !hasPrev || duration <= 0 {
		return diskIORates{}
	}

	rates := diskIORates{
		sampleIntervalSeconds: duration,
		hasSampleInterval:     true,
	}
	if readBytesDiff, ok := counterDiff(curr.readBytes, prev.readBytes); ok {
		rates.readBytesRate = float64(readBytesDiff) / duration
	}
	if writeBytesDiff, ok := counterDiff(curr.writeBytes, prev.writeBytes); ok {
		rates.writeBytesRate = float64(writeBytesDiff) / duration
	}
	if readsDiff, ok := counterDiff(curr.readsCompleted, prev.readsCompleted); ok {
		rates.readsRate = float64(readsDiff) / duration
		if readTimeDiff, ok := counterDiff(curr.readTimeMs, prev.readTimeMs); ok && readsDiff > 0 {
			rates.readLatencyAvg = (float64(readTimeDiff) / 1000.0) / float64(readsDiff)
			rates.hasReadLatency = true
		}
	}
	if writesDiff, ok := counterDiff(curr.writesCompleted, prev.writesCompleted); ok {
		rates.writesRate = float64(writesDiff) / duration
		if writeTimeDiff, ok := counterDiff(curr.writeTimeMs, prev.writeTimeMs); ok && writesDiff > 0 {
			rates.writeLatencyAvg = (float64(writeTimeDiff) / 1000.0) / float64(writesDiff)
			rates.hasWriteLatency = true
		}
	}
	if ioTimeDiff, ok := counterDiff(curr.ioTimeMs, prev.ioTimeMs); ok {
		durationMs := duration * 1000.0
		rates.utilPercent = (float64(ioTimeDiff) / durationMs) * 100.0
		if rates.utilPercent > 100.0 {
			rates.utilPercent = 100.0
		}
	}

	return rates
}

func buildDiskIOEvent(device string, stats diskRawIO, rates diskIORates, mounts any) Event {
	parentDevice := parentBlockDevice(device)
	var parentDeviceValue any
	if parentDevice != "" {
		parentDeviceValue = parentDevice
	}

	return Event{
		Event:     eventMetricSample,
		Collector: "disk_io",
		Component: componentCollector,
		Data: map[string]any{
			"resource_type":                  resourceTypeDevice,
			"resource_id":                    resourceID(resourceTypeDevice, device),
			"device":                         device,
			"device_type":                    diskDeviceType(device),
			"parent_device":                  parentDeviceValue,
			"mounts":                         mounts,
			"sample_interval_seconds":        optionalRoundedFloat(rates.hasSampleInterval, rates.sampleIntervalSeconds, 3),
			"disk_read_bytes_total":          stats.readBytes,
			"disk_write_bytes_total":         stats.writeBytes,
			"disk_reads_completed_total":     stats.readsCompleted,
			"disk_writes_completed_total":    stats.writesCompleted,
			"disk_read_bytes_per_second":     optionalRoundedFloat(rates.hasSampleInterval, rates.readBytesRate, 2),
			"disk_write_bytes_per_second":    optionalRoundedFloat(rates.hasSampleInterval, rates.writeBytesRate, 2),
			"disk_reads_per_second":          optionalRoundedFloat(rates.hasSampleInterval, rates.readsRate, 2),
			"disk_writes_per_second":         optionalRoundedFloat(rates.hasSampleInterval, rates.writesRate, 2),
			"disk_read_latency_seconds_avg":  optionalRoundedFloat(rates.hasReadLatency, rates.readLatencyAvg, 6),
			"disk_write_latency_seconds_avg": optionalRoundedFloat(rates.hasWriteLatency, rates.writeLatencyAvg, 6),
			"disk_io_time_seconds_total":     round(float64(stats.ioTimeMs)/1000.0, 3),
			"disk_io_util_percent":           optionalRoundedFloat(rates.hasSampleInterval, rates.utilPercent, 2),
		},
	}
}

func (c *DiskIOCollector) collectMountMappings() (map[string][]string, bool) {
	if c.mountsPath == "" {
		return nil, false
	}

	mountsByDevice, err := parseDiskIOMountMappings(c.mountsPath)
	if err != nil {
		c.issueLogger().log(slog.LevelDebug, c.Name(), actionOmitMetric, "mounts", c.mountsPath, err)
		return nil, false
	}
	c.issueLogger().clear(c.Name(), actionOmitMetric, "mounts", c.mountsPath)
	return mountsByDevice, true
}

func parseDiskIOMountMappings(path string) (map[string][]string, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	mountsByDevice := make(map[string][]string)
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) < 2 {
			continue
		}
		device := decodeProcMountField(fields[0])
		if !strings.HasPrefix(device, "/dev/") {
			continue
		}
		deviceName := filepath.Base(device)
		mount := decodeProcMountField(fields[1])
		mountsByDevice[deviceName] = append(mountsByDevice[deviceName], mount)
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	for device := range mountsByDevice {
		sort.Strings(mountsByDevice[device])
	}
	return mountsByDevice, nil
}

func diskIOMountsForDevice(device string, mountsByDevice map[string][]string, hasMountMappings bool) any {
	if !hasMountMappings {
		return nil
	}

	mounts := append([]string(nil), mountsByDevice[device]...)
	if diskDeviceType(device) == "disk" {
		for mappedDevice, mappedMounts := range mountsByDevice {
			if parentBlockDevice(mappedDevice) == device {
				mounts = append(mounts, mappedMounts...)
			}
		}
	}
	sort.Strings(mounts)
	return uniqueStrings(mounts)
}

func uniqueStrings(values []string) []string {
	if len(values) == 0 {
		return []string{}
	}
	unique := values[:1]
	for _, value := range values[1:] {
		if value != unique[len(unique)-1] {
			unique = append(unique, value)
		}
	}
	return unique
}

func parseProcDiskstats(path string) (map[string]diskRawIO, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	res := make(map[string]diskRawIO)
	scanner := bufio.NewScanner(file)
	sawPhysicalDisk := false
	for scanner.Scan() {
		device, stat, ok, physical := parseDiskstatsLineWithStatus(scanner.Text())
		if physical {
			sawPhysicalDisk = true
		}
		if !ok {
			continue
		}
		res[device] = stat
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	if sawPhysicalDisk && len(res) == 0 {
		return nil, fmt.Errorf("no usable physical diskstats lines")
	}
	return res, nil
}

func parseDiskstatsLine(line string) (string, diskRawIO, bool) {
	device, stat, ok, _ := parseDiskstatsLineWithStatus(line)
	return device, stat, ok
}

func parseDiskstatsLineWithStatus(line string) (string, diskRawIO, bool, bool) {
	fields := strings.Fields(line)
	if len(fields) < 14 {
		return "", diskRawIO{}, false, false
	}

	device := fields[2]
	if !isPhysicalDisk(device) {
		return "", diskRawIO{}, false, false
	}

	readsCompleted, err := strconv.ParseUint(fields[3], 10, 64)
	if err != nil {
		return "", diskRawIO{}, false, true
	}
	sectorsRead, err := strconv.ParseUint(fields[5], 10, 64)
	if err != nil {
		return "", diskRawIO{}, false, true
	}
	readTimeMs, err := strconv.ParseUint(fields[6], 10, 64)
	if err != nil {
		return "", diskRawIO{}, false, true
	}
	writesCompleted, err := strconv.ParseUint(fields[7], 10, 64)
	if err != nil {
		return "", diskRawIO{}, false, true
	}
	sectorsWritten, err := strconv.ParseUint(fields[9], 10, 64)
	if err != nil {
		return "", diskRawIO{}, false, true
	}
	writeTimeMs, err := strconv.ParseUint(fields[10], 10, 64)
	if err != nil {
		return "", diskRawIO{}, false, true
	}
	ioTimeMs, err := strconv.ParseUint(fields[12], 10, 64)
	if err != nil {
		return "", diskRawIO{}, false, true
	}

	return device, diskRawIO{
		readsCompleted:  readsCompleted,
		readBytes:       sectorsRead * 512,
		readTimeMs:      readTimeMs,
		writesCompleted: writesCompleted,
		writeBytes:      sectorsWritten * 512,
		writeTimeMs:     writeTimeMs,
		ioTimeMs:        ioTimeMs,
	}, true, true
}

func diskIOCounterReset(curr, prev diskRawIO) bool {
	return curr.readsCompleted < prev.readsCompleted ||
		curr.readBytes < prev.readBytes ||
		curr.readTimeMs < prev.readTimeMs ||
		curr.writesCompleted < prev.writesCompleted ||
		curr.writeBytes < prev.writeBytes ||
		curr.writeTimeMs < prev.writeTimeMs ||
		curr.ioTimeMs < prev.ioTimeMs
}

func diskDeviceType(device string) string {
	if strings.HasPrefix(device, "dm-") {
		return "mapped"
	}
	if parentBlockDevice(device) != "" {
		return "partition"
	}
	return "disk"
}

func parentBlockDevice(device string) string {
	if strings.HasPrefix(device, "nvme") {
		idx := strings.LastIndex(device, "p")
		if idx > 0 && hasOnlyDigits(device[idx+1:]) {
			return device[:idx]
		}
		return ""
	}

	idx := len(device)
	for idx > 0 && device[idx-1] >= '0' && device[idx-1] <= '9' {
		idx--
	}
	if idx > 0 && idx < len(device) {
		return device[:idx]
	}
	return ""
}

func hasOnlyDigits(value string) bool {
	if value == "" {
		return false
	}
	for _, ch := range value {
		if ch < '0' || ch > '9' {
			return false
		}
	}
	return true
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
