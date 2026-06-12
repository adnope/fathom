package collector

import (
	"bufio"
	"context"
	"fmt"
	"log/slog"
	"os"
	"slices"
	"sort"
	"strings"
	"sync"
	"syscall"

	"fathom/internal/config"
)

// MountInfo captures metadata about a partition mount point.
type MountInfo struct {
	Mount          string `json:"mount"`
	Device         string `json:"device"`
	FilesystemType string `json:"filesystem_type"`
	Readonly       bool   `json:"readonly"`
}

// DiskCollector monitors disk usage, inodes, and mount points metadata.
type DiskCollector struct {
	mu           sync.Mutex
	mountsPath   string
	cachedMounts map[string]MountInfo
	hasSnapshot  bool
	config       *config.DiskConfig
	issues       *collectorIssueLogger
}

// NewDiskCollector constructs a DiskCollector monitoring /proc/mounts.
func NewDiskCollector(cfg *config.DiskConfig) *DiskCollector {
	return &DiskCollector{
		mountsPath:   "/proc/mounts",
		cachedMounts: make(map[string]MountInfo),
		config:       cfg,
		issues:       newCollectorIssueLogger(),
	}
}

// Name returns the name of the collector.
func (c *DiskCollector) Name() string {
	return "disk"
}

// UpdateConfig updates the active disk filtering configuration block dynamically.
func (c *DiskCollector) UpdateConfig(cfg *config.DiskConfig) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.config = cfg
}

// Collect returns mount metadata snapshots, change events, and dynamic storage metric events.
func (c *DiskCollector) Collect(ctx context.Context) ([]Event, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	currentMounts, err := c.parseMounts()
	if err != nil {
		return nil, fmt.Errorf("read required disk source %s: %w", c.mountsPath, err)
	}

	var events []Event

	if !c.hasSnapshot {
		events = append(events, buildDiskMetadataSnapshotEvent(currentMounts))
		c.cachedMounts = currentMounts
		c.hasSnapshot = true
	} else {
		events = append(events, buildDiskMetadataChangeEvents(c.cachedMounts, currentMounts)...)
		c.cachedMounts = currentMounts
	}

	for _, mPath := range sortedMountPaths(currentMounts) {
		var stat syscall.Statfs_t
		if err := syscall.Statfs(mPath, &stat); err != nil {
			level := slog.LevelDebug
			if c.isConfiguredMount(mPath) {
				level = slog.LevelWarn
			}
			c.issueLogger().log(level, c.Name(), actionSkipResource, "disk_total_bytes", mPath, err,
				slog.String("resource_type", resourceTypeMount),
				slog.String("resource", mPath),
			)
			continue
		}
		c.issueLogger().clear(c.Name(), actionSkipResource, "disk_total_bytes", mPath,
			slog.String("resource_type", resourceTypeMount),
			slog.String("resource", mPath),
		)

		data, ok := diskMetricDataFromStatfs(currentMounts[mPath], stat)
		if !ok {
			continue
		}

		events = append(events, Event{
			Event:     eventMetricSample,
			Collector: c.Name(),
			Component: componentCollector,
			Data:      data,
		})
	}

	return events, nil
}

func (c *DiskCollector) issueLogger() *collectorIssueLogger {
	if c.issues == nil {
		c.issues = newCollectorIssueLogger()
	}
	return c.issues
}

func (c *DiskCollector) isConfiguredMount(mountPath string) bool {
	return c.config != nil && slices.Contains(c.config.IncludeMounts, mountPath)
}

func sortedMountPaths(mounts map[string]MountInfo) []string {
	var paths []string
	for mountPath := range mounts {
		paths = append(paths, mountPath)
	}
	sort.Strings(paths)
	return paths
}

func buildDiskMetadataSnapshotEvent(mounts map[string]MountInfo) Event {
	var mountsList []any
	for _, mountPath := range sortedMountPaths(mounts) {
		mount := mounts[mountPath]
		mountsList = append(mountsList, map[string]any{
			"mount":           mount.Mount,
			"device":          mount.Device,
			"filesystem_type": mount.FilesystemType,
			"readonly":        mount.Readonly,
		})
	}

	return Event{
		Event:     "disk_metadata_snapshot",
		Collector: "disk",
		Component: componentCollector,
		Data: map[string]any{
			"mounts": mountsList,
		},
	}
}

func buildDiskMetadataChangeEvents(oldMounts, currentMounts map[string]MountInfo) []Event {
	var events []Event
	for _, mountPath := range sortedMountPaths(oldMounts) {
		if _, exists := currentMounts[mountPath]; exists {
			continue
		}

		events = append(events, Event{
			Event:     "disk_metadata_changed",
			Collector: "disk",
			Component: componentCollector,
			Data: map[string]any{
				"mount":          mountPath,
				"old":            oldMounts[mountPath],
				"new":            nil,
				"changed_fields": []string{"mount_removed"},
			},
		})
	}

	for _, mountPath := range sortedMountPaths(currentMounts) {
		newMount := currentMounts[mountPath]
		oldMount, exists := oldMounts[mountPath]
		if !exists {
			events = append(events, Event{
				Event:     "disk_metadata_changed",
				Collector: "disk",
				Component: componentCollector,
				Data: map[string]any{
					"mount":          mountPath,
					"old":            nil,
					"new":            newMount,
					"changed_fields": []string{"mount_added"},
				},
			})
			continue
		}

		changed := changedDiskMountFields(oldMount, newMount)
		if len(changed) == 0 {
			continue
		}

		events = append(events, Event{
			Event:     "disk_metadata_changed",
			Collector: "disk",
			Component: componentCollector,
			Data: map[string]any{
				"mount":          mountPath,
				"old":            oldMount,
				"new":            newMount,
				"changed_fields": changed,
			},
		})
	}

	return events
}

func changedDiskMountFields(oldMount, newMount MountInfo) []string {
	var changed []string
	if oldMount.Readonly != newMount.Readonly {
		changed = append(changed, "readonly")
	}
	if oldMount.Device != newMount.Device {
		changed = append(changed, "device")
	}
	return changed
}

func diskMetricDataFromStatfs(mount MountInfo, stat syscall.Statfs_t) (map[string]any, bool) {
	total := stat.Blocks * uint64(stat.Bsize)
	if total == 0 {
		return nil, false
	}

	free := stat.Bfree * uint64(stat.Bsize)
	available := stat.Bavail * uint64(stat.Bsize)
	reserved, _ := nonNegativeDelta(free, available)

	var used uint64
	if total > free {
		used = total - free
	}

	// Some filesystems, such as FAT/VFAT, report zero inodes.
	var inodesTotal, inodesFree, inodesUsed, inodesUsedPercent any
	supportsInodes := false
	if stat.Files > 0 {
		supportsInodes = true
		inodesTotal = stat.Files
		inodesFree = stat.Ffree
		usedFiles, _ := nonNegativeDelta(stat.Files, stat.Ffree)
		inodesUsed = usedFiles
		inodesUsedPercent = round((float64(usedFiles)/float64(stat.Files))*100, 2)
	}

	return map[string]any{
		"resource_type":          resourceTypeMount,
		"resource_id":            resourceID(resourceTypeMount, mount.Mount),
		"mount":                  mount.Mount,
		"device":                 mount.Device,
		"filesystem_type":        mount.FilesystemType,
		"readonly":               mount.Readonly,
		"disk_total_bytes":       total,
		"disk_used_bytes":        used,
		"disk_free_bytes":        free,
		"disk_available_bytes":   available,
		"disk_reserved_bytes":    reserved,
		"disk_used_percent":      round((float64(used)/float64(total))*100, 2),
		"disk_free_percent":      round((float64(free)/float64(total))*100, 2),
		"disk_available_percent": round((float64(available)/float64(total))*100, 2),
		"disk_reserved_percent":  round((float64(reserved)/float64(total))*100, 2),
		"supports_inodes":        supportsInodes,
		"inodes_total":           inodesTotal,
		"inodes_used":            inodesUsed,
		"inodes_free":            inodesFree,
		"inodes_used_percent":    inodesUsedPercent,
	}, true
}

func (c *DiskCollector) shouldKeepMount(mount, fsType string) bool {
	if c.config != nil && len(c.config.IncludeMounts) > 0 {
		return slices.Contains(c.config.IncludeMounts, mount)
	}

	includeVirtual := false
	if c.config != nil && c.config.IncludeVirtual != nil {
		includeVirtual = *c.config.IncludeVirtual
	}

	if !includeVirtual {
		excludeFS := []string{"tmpfs", "devtmpfs", "proc", "sysfs", "cgroup", "cgroup2", "debugfs", "tracefs", "securityfs", "pstore", "efivarfs", "overlay", "squashfs", "autofs", "mqueue", "hugetlbfs", "fusectl"}
		if c.config != nil && len(c.config.ExcludeFilesystems) > 0 {
			excludeFS = c.config.ExcludeFilesystems
		}
		if slices.Contains(excludeFS, fsType) {
			return false
		}
	} else {
		if fsType == "proc" || fsType == "sysfs" {
			return false
		}
	}

	excludePrefixes := []string{"/run", "/dev", "/proc", "/sys"}
	if c.config != nil && len(c.config.ExcludeMountPrefixes) > 0 {
		excludePrefixes = c.config.ExcludeMountPrefixes
	}

	for _, prefix := range excludePrefixes {
		if strings.HasPrefix(mount, prefix) {
			return false
		}
	}

	return true
}

func (c *DiskCollector) parseMounts() (map[string]MountInfo, error) {
	file, err := os.Open(c.mountsPath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	mounts := make(map[string]MountInfo)
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		fields := strings.Fields(line)
		if len(fields) < 4 {
			continue
		}
		device := decodeProcMountField(fields[0])
		mount := decodeProcMountField(fields[1])
		fsType := fields[2]
		opts := fields[3]

		if !c.shouldKeepMount(mount, fsType) {
			continue
		}

		if _, exists := mounts[mount]; exists {
			continue
		}

		mounts[mount] = MountInfo{
			Mount:          mount,
			Device:         device,
			FilesystemType: fsType,
			Readonly:       isReadonly(opts),
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return mounts, nil
}

func decodeProcMountField(field string) string {
	return strings.NewReplacer(
		`\040`, " ",
		`\011`, "\t",
		`\012`, "\n",
		`\134`, `\`,
	).Replace(field)
}

func isReadonly(opts string) bool {
	parts := strings.SplitSeq(opts, ",")
	for opt := range parts {
		if opt == "ro" {
			return true
		}
	}
	return false
}
