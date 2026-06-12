package collector

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"syscall"
	"testing"
)

func TestDiskCollector(t *testing.T) {
	tmpDir := t.TempDir()
	mountsFile := filepath.Join(tmpDir, "mounts")

	content := `/dev/nvme0n1p2 / ext4 rw,relatime 0 0
/dev/nvme0n1p1 /boot vfat rw,relatime 0 0
proc /proc proc rw,relatime 0 0
`
	err := os.WriteFile(mountsFile, []byte(content), 0644)
	if err != nil {
		t.Fatalf("failed to write mock mounts: %v", err)
	}

	t.Run("mounts snapshot and collection", func(t *testing.T) {
		c := &DiskCollector{
			mountsPath:   mountsFile,
			cachedMounts: make(map[string]MountInfo),
		}

		events, err := c.Collect(context.Background())
		if err != nil {
			t.Fatalf("Collect failed: %v", err)
		}

		if len(events) < 1 {
			t.Fatal("expected at least 1 snapshot event")
		}

		var snapshotEv Event
		var metricSamples []Event
		for _, ev := range events {
			switch ev.Event {
			case "disk_metadata_snapshot":
				snapshotEv = ev
			case "metric_sample":
				metricSamples = append(metricSamples, ev)
			}
		}

		if snapshotEv.Event == "" {
			t.Error("missing disk_metadata_snapshot event")
		}

		mounts := snapshotEv.Data["mounts"].([]any)
		if len(mounts) != 2 {
			t.Errorf("expected 2 mounts in snapshot, got %d", len(mounts))
		}

		for _, m := range mounts {
			mMap := m.(map[string]any)
			if mMap["readonly"].(bool) {
				t.Error("expected mock mounts to be read-write, got readonly")
			}
		}
	})
}

func TestBuildDiskMetadataSnapshotEventSortsMounts(t *testing.T) {
	event := buildDiskMetadataSnapshotEvent(map[string]MountInfo{
		"/var":  {Mount: "/var", Device: "/dev/sdb1", FilesystemType: "ext4"},
		"/boot": {Mount: "/boot", Device: "/dev/sda1", FilesystemType: "vfat"},
	})

	mounts := event.Data["mounts"].([]any)
	if got := mounts[0].(map[string]any)["mount"]; got != "/boot" {
		t.Fatalf("expected first mount /boot, got %v", got)
	}
	if got := mounts[1].(map[string]any)["mount"]; got != "/var" {
		t.Fatalf("expected second mount /var, got %v", got)
	}
}

func TestBuildDiskMetadataChangeEvents(t *testing.T) {
	oldMounts := map[string]MountInfo{
		"/same":       {Mount: "/same", Device: "/dev/sda1", FilesystemType: "ext4"},
		"/changed":    {Mount: "/changed", Device: "/dev/sda2", FilesystemType: "ext4"},
		"/fs_changed": {Mount: "/fs_changed", Device: "/dev/sda3", FilesystemType: "ext4"},
		"/removed":    {Mount: "/removed", Device: "/dev/sda4", FilesystemType: "ext4"},
	}
	currentMounts := map[string]MountInfo{
		"/same":       {Mount: "/same", Device: "/dev/sda1", FilesystemType: "ext4"},
		"/added":      {Mount: "/added", Device: "/dev/sdb1", FilesystemType: "ext4"},
		"/changed":    {Mount: "/changed", Device: "/dev/sdc1", FilesystemType: "ext4", Readonly: true},
		"/fs_changed": {Mount: "/fs_changed", Device: "/dev/sda3", FilesystemType: "xfs"},
	}

	events := buildDiskMetadataChangeEvents(oldMounts, currentMounts)

	if len(events) != 3 {
		t.Fatalf("expected 3 change events, got %d", len(events))
	}
	assertDiskChangeEvent(t, events[0], "/removed", []string{"mount_removed"})
	assertDiskChangeEvent(t, events[1], "/added", []string{"mount_added"})
	assertDiskChangeEvent(t, events[2], "/changed", []string{"readonly", "device"})
}

func TestDiskMetricDataFromStatfs(t *testing.T) {
	data, ok := diskMetricDataFromStatfs(MountInfo{Mount: "/data", Device: "/dev/sdb1", FilesystemType: "ext4", Readonly: true}, syscall.Statfs_t{
		Bsize:  1024,
		Blocks: 10,
		Bfree:  4,
		Bavail: 3,
		Files:  100,
		Ffree:  25,
	})
	if !ok {
		t.Fatal("expected statfs metrics")
	}

	expected := map[string]any{
		"resource_type":          "mount",
		"resource_id":            "mount:/data",
		"mount":                  "/data",
		"device":                 "/dev/sdb1",
		"filesystem_type":        "ext4",
		"readonly":               true,
		"disk_total_bytes":       uint64(10240),
		"disk_used_bytes":        uint64(6144),
		"disk_free_bytes":        uint64(4096),
		"disk_available_bytes":   uint64(3072),
		"disk_reserved_bytes":    uint64(1024),
		"disk_used_percent":      60.0,
		"disk_free_percent":      40.0,
		"disk_available_percent": 30.0,
		"disk_reserved_percent":  10.0,
		"supports_inodes":        true,
		"inodes_total":           uint64(100),
		"inodes_used":            uint64(75),
		"inodes_free":            uint64(25),
		"inodes_used_percent":    75.0,
	}
	for key, want := range expected {
		if got := data[key]; got != want {
			t.Errorf("for %s: expected %v, got %v", key, want, got)
		}
	}

	data, ok = diskMetricDataFromStatfs(MountInfo{Mount: "/fat"}, syscall.Statfs_t{Bsize: 1024, Blocks: 1})
	if !ok {
		t.Fatal("expected statfs metrics without inodes")
	}
	if data["supports_inodes"] != false {
		t.Fatalf("expected supports_inodes false, got %+v", data)
	}
	if data["inodes_total"] != nil || data["inodes_used"] != nil || data["inodes_free"] != nil || data["inodes_used_percent"] != nil {
		t.Fatalf("expected inode fields to be nil, got %+v", data)
	}

	if _, ok := diskMetricDataFromStatfs(MountInfo{Mount: "/empty"}, syscall.Statfs_t{}); ok {
		t.Fatal("expected zero-size filesystem to be skipped")
	}
}

func assertDiskChangeEvent(t *testing.T, event Event, mount string, changedFields []string) {
	t.Helper()

	if event.Event != "disk_metadata_changed" || event.Collector != "disk" {
		t.Fatalf("unexpected event headers: %+v", event)
	}
	if event.Data["mount"] != mount {
		t.Fatalf("expected mount %s, got %v", mount, event.Data["mount"])
	}
	if got := event.Data["changed_fields"].([]string); !slices.Equal(got, changedFields) {
		t.Fatalf("expected changed fields %v, got %v", changedFields, got)
	}
}
