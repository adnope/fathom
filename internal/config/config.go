package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Agent AgentConfig `yaml:"agent"`
}

type AgentConfig struct {
	Version         string `yaml:"version"`
	LogLevel        string `yaml:"log_level"`
	CollectInterval string `yaml:"collect_interval"`
}

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

func (c *Config) Validate() error {
	if c.Agent.Version == "" {
		return fmt.Errorf("agent version is required")
	}

	switch c.Agent.LogLevel {
	case "debug", "info", "warn", "error", "":
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
