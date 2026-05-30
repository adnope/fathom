package collector

import (
	"context"
	"os"
	"path/filepath"
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
