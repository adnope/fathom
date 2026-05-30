package collector

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"reflect"
	"strings"
	"sync"
	"syscall"
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
}

// NewDiskCollector constructs a DiskCollector monitoring /proc/mounts.
func NewDiskCollector() *DiskCollector {
	return &DiskCollector{
		mountsPath:   "/proc/mounts",
		cachedMounts: make(map[string]MountInfo),
	}
}

// Name returns the name of the collector.
func (c *DiskCollector) Name() string {
	return "disk"
}

var whitelistedFS = map[string]bool{
	"ext2":    true,
	"ext3":    true,
	"ext4":    true,
	"xfs":     true,
	"btrfs":   true,
	"vfat":    true,
	"exfat":   true,
	"ntfs":    true,
	"fuseblk": true,
	"zfs":     true,
	"tmpfs":   true,
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
		var mountsList []interface{}
		for _, m := range currentMounts {
			mountsList = append(mountsList, map[string]interface{}{
				"mount":           m.Mount,
				"device":          m.Device,
				"filesystem_type": m.FilesystemType,
				"readonly":        m.Readonly,
			})
		}
		events = append(events, Event{
			Event:     "disk_metadata_snapshot",
			Collector: "disk",
			Data: map[string]interface{}{
				"mounts": mountsList,
			},
		})
		c.cachedMounts = currentMounts
		c.hasSnapshot = true
	} else {
		// Detect changes
		for mPath, oldM := range c.cachedMounts {
			if _, exists := currentMounts[mPath]; !exists {
				events = append(events, Event{
					Event:     "disk_metadata_changed",
					Collector: "disk",
					Component: "collector",
					Data: map[string]interface{}{
						"mount":          mPath,
						"old":            oldM,
						"new":            nil,
						"changed_fields": []string{"mount_removed"},
					},
				})
			}
		}

		for mPath, newM := range currentMounts {
			oldM, exists := c.cachedMounts[mPath]
			if !exists {
				events = append(events, Event{
					Event:     "disk_metadata_changed",
					Collector: "disk",
					Component: "collector",
					Data: map[string]interface{}{
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
						Data: map[string]interface{}{
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

	// Gather metrics for active mounts
	for mPath := range currentMounts {
		var stat syscall.Statfs_t
		if err := syscall.Statfs(mPath, &stat); err != nil {
			continue // Skip mounts with errors (e.g. permission or unmounted during check)
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

		usedPercent := (float64(used) / float64(total)) * 100
		freePercent := (float64(free) / float64(total)) * 100
		availablePercent := (float64(available) / float64(total)) * 100

		inodesTotal := stat.Files
		inodesFree := stat.Ffree
		inodesUsed := inodesTotal - inodesFree

		var inodesUsedPercent float64
		if inodesTotal > 0 {
			inodesUsedPercent = (float64(inodesUsed) / float64(inodesTotal)) * 100
		}

		events = append(events, Event{
			Event:     "metric_sample",
			Collector: "disk",
			Data: map[string]interface{}{
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

		if !whitelistedFS[fsType] {
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
	return mounts, scanner.Err()
}

func isReadonly(opts string) bool {
	parts := strings.Split(opts, ",")
	for _, opt := range parts {
		if opt == "ro" {
			return true
		}
	}
	return false
}
