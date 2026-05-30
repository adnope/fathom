package collector

import (
	"bufio"
	"context"
	"fmt"
	"log/slog"
	"os"
	"reflect"
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
}

// NewDiskCollector constructs a DiskCollector monitoring /proc/mounts.
func NewDiskCollector(cfg *config.DiskConfig) *DiskCollector {
	return &DiskCollector{
		mountsPath:   "/proc/mounts",
		cachedMounts: make(map[string]MountInfo),
		config:       cfg,
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

	currentMounts, err := c.parseMounts()
	if err != nil {
		return nil, fmt.Errorf("failed to parse mounts: %w", err)
	}

	var events []Event

	if !c.hasSnapshot {
		var mountsList []any
		for _, m := range currentMounts {
			mountsList = append(mountsList, map[string]any{
				"mount":           m.Mount,
				"device":          m.Device,
				"filesystem_type": m.FilesystemType,
				"readonly":        m.Readonly,
			})
		}
		// Sort alphabetically by mount path
		sort.Slice(mountsList, func(i, j int) bool {
			return mountsList[i].(map[string]any)["mount"].(string) < mountsList[j].(map[string]any)["mount"].(string)
		})

		events = append(events, Event{
			Event:     "disk_metadata_snapshot",
			Collector: "disk",
			Component: "collector",
			Data: map[string]any{
				"mounts": mountsList,
			},
		})
		c.cachedMounts = currentMounts
		c.hasSnapshot = true
	} else {
		// Detect changes (sorted alphabetically for stable logs order)
		var removedKeys []string
		for mPath := range c.cachedMounts {
			if _, exists := currentMounts[mPath]; !exists {
				removedKeys = append(removedKeys, mPath)
			}
		}
		sort.Strings(removedKeys)

		for _, mPath := range removedKeys {
			events = append(events, Event{
				Event:     "disk_metadata_changed",
				Collector: "disk",
				Component: "collector",
				Data: map[string]any{
					"mount":          mPath,
					"old":            c.cachedMounts[mPath],
					"new":            nil,
					"changed_fields": []string{"mount_removed"},
				},
			})
		}

		var addedOrModifiedKeys []string
		for mPath := range currentMounts {
			addedOrModifiedKeys = append(addedOrModifiedKeys, mPath)
		}
		sort.Strings(addedOrModifiedKeys)

		for _, mPath := range addedOrModifiedKeys {
			newM := currentMounts[mPath]
			oldM, exists := c.cachedMounts[mPath]
			if !exists {
				events = append(events, Event{
					Event:     "disk_metadata_changed",
					Collector: "disk",
					Component: "collector",
					Data: map[string]any{
						"mount":          mPath,
						"old":            nil,
						"new":            newM,
						"changed_fields": []string{"mount_added"},
					},
				})
			} else if !reflect.DeepEqual(oldM, newM) {
				var changed []string
				if oldM.Readonly != newM.Readonly {
					changed = append(changed, "readonly")
				}
				if oldM.Device != newM.Device {
					changed = append(changed, "device")
				}
				if len(changed) > 0 {
					events = append(events, Event{
						Event:     "disk_metadata_changed",
						Collector: "disk",
						Component: "collector",
						Data: map[string]any{
							"mount":          mPath,
							"old":            oldM,
							"new":            newM,
							"changed_fields": changed,
						},
					})
				}
			}
		}

		c.cachedMounts = currentMounts
	}

	// Gather metrics for active mounts in alphabetical order
	var sortedMounts []string
	for mPath := range currentMounts {
		sortedMounts = append(sortedMounts, mPath)
	}
	sort.Strings(sortedMounts)

	for _, mPath := range sortedMounts {
		var stat syscall.Statfs_t
		if err := syscall.Statfs(mPath, &stat); err != nil {
			slog.Debug("failed to query space statistics for mount",
				slog.String("component", "collector"),
				slog.String("collector", "disk"),
				slog.String("mount", mPath),
				slog.String("error", err.Error()),
			)
			continue // Skip mounts with errors
		}

		total := stat.Blocks * uint64(stat.Bsize)
		free := stat.Bfree * uint64(stat.Bsize)
		available := stat.Bavail * uint64(stat.Bsize)

		if total == 0 {
			continue
		}

		var used uint64
		if total > free {
			used = total - free
		}

		usedPercent := round((float64(used)/float64(total))*100, 2)
		freePercent := round((float64(free)/float64(total))*100, 2)
		availablePercent := round((float64(available)/float64(total))*100, 2)

		// Handle filesystems without inode support (like FAT/VFAT) where total files is reported as 0
		var inodesTotal, inodesFree, inodesUsed, inodesUsedPercent any
		if stat.Files > 0 {
			inodesTotal = stat.Files
			inodesFree = stat.Ffree
			inodesUsed = stat.Files - stat.Ffree
			inodesUsedPercent = round((float64(stat.Files-stat.Ffree)/float64(stat.Files))*100, 2)
		}

		events = append(events, Event{
			Event:     "metric_sample",
			Collector: "disk",
			Component: "collector",
			Data: map[string]any{
				"mount":                  mPath,
				"disk_total_bytes":       total,
				"disk_used_bytes":        used,
				"disk_free_bytes":        free,
				"disk_available_bytes":   available,
				"disk_used_percent":      usedPercent,
				"disk_free_percent":      freePercent,
				"disk_available_percent": availablePercent,
				"inodes_total":           inodesTotal,
				"inodes_used":            inodesUsed,
				"inodes_free":            inodesFree,
				"inodes_used_percent":    inodesUsedPercent,
			},
		})
	}

	return events, nil
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
		device := fields[0]
		mount := fields[1]
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

func isReadonly(opts string) bool {
	parts := strings.SplitSeq(opts, ",")
	for opt := range parts {
		if opt == "ro" {
			return true
		}
	}
	return false
}
