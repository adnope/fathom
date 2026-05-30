package collector

import "context"

// Event represents a structured observability payload emitted by collectors.
type Event struct {
	Event     string         `json:"event"`
	Component string         `json:"component,omitempty"`
	Collector string         `json:"collector,omitempty"`
	Data      map[string]any `json:"data"`
}

// Collector defines the contract for all system metrics and metadata collectors.
type Collector interface {
	Name() string
	Collect(ctx context.Context) ([]Event, error)
}
