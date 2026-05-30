package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// Config represents the overall configuration structure for the fathom agent.
type Config struct {
	Agent   AgentConfig   `yaml:"agent"`
	Disk    DiskConfig    `yaml:"disk"`
	Network NetworkConfig `yaml:"network"`
}

// AgentConfig represents settings specific to the monitoring agent daemon.
type AgentConfig struct {
	Version         string `yaml:"version"`
	LogLevel        string `yaml:"log_level"`
	CollectInterval string `yaml:"collect_interval"`
}

// DiskConfig controls the filtering and observation parameters for storage mounts.
type DiskConfig struct {
	IncludeVirtual       *bool    `yaml:"include_virtual"`
	IncludeMounts        []string `yaml:"include_mounts"`
	ExcludeFilesystems   []string `yaml:"exclude_filesystems"`
	ExcludeMountPrefixes []string `yaml:"exclude_mount_prefixes"`
}

// NetworkConfig controls interface collection limits and whitelists.
type NetworkConfig struct {
	IncludeLoopback   *bool    `yaml:"include_loopback"`
	IncludeVirtual    *bool    `yaml:"include_virtual"`
	IncludeDown       *bool    `yaml:"include_down"`
	IncludeInterfaces []string `yaml:"include_interfaces"`
	ExcludePrefixes   []string `yaml:"exclude_prefixes"`
}

// Load reads and parses a YAML configuration file from the specified path.
// It also validates the configuration before returning.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse config YAML: %w", err)
	}

	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	return &cfg, nil
}

// Validate checks the configuration for semantic correctness and required parameters.
func (c *Config) Validate() error {
	if c.Agent.Version == "" {
		return fmt.Errorf("agent version is required")
	}

	switch c.Agent.LogLevel {
	case "debug", "info", "warn", "error", "":
		// valid levels
	default:
		return fmt.Errorf("unsupported log level %q (must be debug, info, warn, or error)", c.Agent.LogLevel)
	}

	if c.Agent.CollectInterval == "" {
		return fmt.Errorf("collect_interval is required")
	}
	if _, err := time.ParseDuration(c.Agent.CollectInterval); err != nil {
		return fmt.Errorf("invalid collect_interval: %w", err)
	}

	return nil
}
