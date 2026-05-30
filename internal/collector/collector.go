package collector

import (
	"context"
	"math"
)

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

// round formats a float64 to a specified number of decimal places.
func round(val float64, decimals int) float64 {
	ratio := math.Pow(10, float64(decimals))
	return math.Round(val*ratio) / ratio
}
