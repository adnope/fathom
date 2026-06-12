package collector

import (
	"context"
	"log/slog"
	"math"
	"strings"
	"sync"
)

const (
	componentCollector = "collector"

	eventMetricSample        = "metric_sample"
	eventCollectorIssue      = "collector_issue"
	eventCollectorError      = "collector_error"
	eventCollectorRecovered  = "collector_recovered"
	actionFailCollect        = "fail_collect"
	actionOmitMetric         = "omit_metric"
	actionSkipResource       = "skip_resource"
	actionZeroMetric         = "zero_metric"
	actionCancelCollect      = "cancel_collect"
	actionRecoverCollect     = "recover_collect"
	resourceTypeMount        = "mount"
	resourceTypeInterface    = "interface"
	resourceTypeDevice       = "device"
	resourceTypeCPU          = "cpu"
	metricTemperatureCelsius = "cpu_temperature_celsius"
	metricFrequencyMHz       = "cpu_frequency_mhz"
	metricPowerWatts         = "cpu_power_watts"
)

// Event represents a structured payload emitted by collectors.
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

type collectorIssueLogger struct {
	mu     sync.Mutex
	active map[string]bool
}

func newCollectorIssueLogger() *collectorIssueLogger {
	return &collectorIssueLogger{active: make(map[string]bool)}
}

func (l *collectorIssueLogger) log(level slog.Level, collectorName, action, metric, source string, err error, attrs ...slog.Attr) {
	if l == nil {
		return
	}

	key := collectorIssueKey(collectorName, action, metric, source, attrs)
	l.mu.Lock()
	if l.active[key] {
		l.mu.Unlock()
		return
	}
	l.active[key] = true
	l.mu.Unlock()

	logAttrs := []slog.Attr{
		slog.String("component", componentCollector),
		slog.String("collector", collectorName),
		slog.String("action", action),
	}
	if metric != "" {
		logAttrs = append(logAttrs, slog.String("metric", metric))
	}
	if source != "" {
		logAttrs = append(logAttrs, slog.String("source", source))
	}
	if err != nil {
		logAttrs = append(logAttrs, slog.String("error", err.Error()))
	}
	logAttrs = append(logAttrs, attrs...)

	slog.Default().LogAttrs(context.Background(), level, eventCollectorIssue, logAttrs...)
}

func (l *collectorIssueLogger) clear(collectorName, action, metric, source string, attrs ...slog.Attr) {
	if l == nil {
		return
	}

	key := collectorIssueKey(collectorName, action, metric, source, attrs)
	l.mu.Lock()
	delete(l.active, key)
	l.mu.Unlock()
}

func collectorIssueKey(collectorName, action, metric, source string, attrs []slog.Attr) string {
	parts := []string{collectorName, action, metric, source, "", ""}
	for _, attr := range attrs {
		switch attr.Key {
		case "resource_type":
			parts[4] = attr.Value.String()
		case "resource":
			parts[5] = attr.Value.String()
		}
	}
	return strings.Join(parts, "\x00")
}

func nonNegativeDelta(curr, prev uint64) (uint64, bool) {
	if curr < prev {
		return 0, false
	}
	return curr - prev, true
}

func counterDiff(curr, prev uint64) (uint64, bool) {
	return nonNegativeDelta(curr, prev)
}

// round formats a float64 to a specified number of decimal places.
func round(val float64, decimals int) float64 {
	ratio := math.Pow(10, float64(decimals))
	return math.Round(val*ratio) / ratio
}

func optionalRoundedFloat(hasValue bool, value float64, decimals int) any {
	if !hasValue {
		return nil
	}
	return round(value, decimals)
}

func resourceID(resourceType, resource string) string {
	return resourceType + ":" + resource
}
