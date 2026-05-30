package config

import (
	"os"
	"path/filepath"
	"testing"
)

func writeTempFile(t *testing.T, content string) string {
	t.Helper()
	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "config.yaml")
	if err := os.WriteFile(tmpFile, []byte(content), 0644); err != nil {
		t.Fatalf("failed to write temp file: %v", err)
	}
	return tmpFile
}

func TestLoad(t *testing.T) {
	t.Run("valid configuration", func(t *testing.T) {
		content := `
agent:
  version: "0.1.0"
  log_level: "info"
  collect_interval: "10s"
`
		tmpFile := writeTempFile(t, content)
		cfg, err := Load(tmpFile)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if cfg.Agent.Version != "0.1.0" {
			t.Errorf("expected version 0.1.0, got %s", cfg.Agent.Version)
		}
		if cfg.Agent.LogLevel != "info" {
			t.Errorf("expected log_level info, got %s", cfg.Agent.LogLevel)
		}
		if cfg.Agent.CollectInterval != "10s" {
			t.Errorf("expected collect_interval 10s, got %s", cfg.Agent.CollectInterval)
		}
	})

	t.Run("missing version", func(t *testing.T) {
		content := `
agent:
  log_level: "info"
  collect_interval: "10s"
`
		tmpFile := writeTempFile(t, content)
		_, err := Load(tmpFile)
		if err == nil {
			t.Fatal("expected error due to missing version, got nil")
		}
	})

	t.Run("invalid log level", func(t *testing.T) {
		content := `
agent:
  version: "0.1.0"
  log_level: "invalid"
  collect_interval: "10s"
`
		tmpFile := writeTempFile(t, content)
		_, err := Load(tmpFile)
		if err == nil {
			t.Fatal("expected error due to invalid log level, got nil")
		}
	})

	t.Run("missing collect_interval", func(t *testing.T) {
		content := `
agent:
  version: "0.1.0"
  log_level: "info"
`
		tmpFile := writeTempFile(t, content)
		_, err := Load(tmpFile)
		if err == nil {
			t.Fatal("expected error due to missing collect_interval, got nil")
		}
	})

	t.Run("invalid collect_interval", func(t *testing.T) {
		content := `
agent:
  version: "0.1.0"
  log_level: "info"
  collect_interval: "10x"
`
		tmpFile := writeTempFile(t, content)
		_, err := Load(tmpFile)
		if err == nil {
			t.Fatal("expected error due to invalid collect_interval, got nil")
		}
	})

	t.Run("invalid YAML syntax", func(t *testing.T) {
		content := `
agent:
  version: "0.1.0"
  log_level: info: error
  collect_interval: "10s"
`
		tmpFile := writeTempFile(t, content)
		_, err := Load(tmpFile)
		if err == nil {
			t.Fatal("expected YAML parsing error, got nil")
		}
	})

	t.Run("missing file", func(t *testing.T) {
		_, err := Load("non_existent_file.yaml")
		if err == nil {
			t.Fatal("expected error for non-existent file, got nil")
		}
	})
}
