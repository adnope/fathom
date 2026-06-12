package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"fathom/internal/collector"
)

type testCollector struct {
	name string
	err  error
}

func (c *testCollector) Name() string {
	return c.name
}

func (c *testCollector) Collect(ctx context.Context) ([]collector.Event, error) {
	if c.err != nil {
		return nil, c.err
	}
	return []collector.Event{{Event: "metric_sample", Component: "collector", Collector: c.name, Data: map[string]any{"ok": true}}}, nil
}

func TestRunCollectionLogsErrorAndRecovery(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	failures := make(map[string]bool)
	c := &testCollector{name: "test", err: errors.New("boom")}

	runCollection(context.Background(), logger, []collector.Collector{c}, failures, "test-host")
	if !strings.Contains(buf.String(), "collector_error") || !strings.Contains(buf.String(), "fail_collect") {
		t.Fatalf("expected collector_error with fail_collect, got %s", buf.String())
	}
	if !strings.Contains(buf.String(), `"host":"test-host"`) {
		t.Fatalf("expected collector_error host label, got %s", buf.String())
	}

	c.err = nil
	runCollection(context.Background(), logger, []collector.Collector{c}, failures, "test-host")
	if !strings.Contains(buf.String(), "collector_recovered") || !strings.Contains(buf.String(), "recover_collect") {
		t.Fatalf("expected collector_recovered with recover_collect, got %s", buf.String())
	}
}

func TestRunCollectionLogsCancellationAtDebug(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	c := &testCollector{name: "test", err: context.Canceled}

	runCollection(context.Background(), logger, []collector.Collector{c}, make(map[string]bool), "test-host")
	logOutput := buf.String()
	if !strings.Contains(logOutput, "collector_error") || !strings.Contains(logOutput, "cancel_collect") {
		t.Fatalf("expected collector_error with cancel_collect, got %s", logOutput)
	}
	if !strings.Contains(logOutput, `"level":"DEBUG"`) {
		t.Fatalf("expected cancellation to log at debug, got %s", logOutput)
	}
}

func TestRunCollectionAddsHostAndSharedCollectionTimestamp(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	collectors := []collector.Collector{
		&testCollector{name: "one"},
		&testCollector{name: "two"},
	}

	runCollection(context.Background(), logger, collectors, make(map[string]bool), "test-host")

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("expected 2 log lines, got %d: %s", len(lines), buf.String())
	}

	var collectionTS string
	for _, line := range lines {
		var record map[string]any
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			t.Fatalf("failed to decode log line %q: %v", line, err)
		}
		if record["host"] != "test-host" {
			t.Fatalf("expected host label, got %+v", record)
		}
		if record["collection_ts"] == "" {
			t.Fatalf("expected collection_ts, got %+v", record)
		}
		if collectionTS == "" {
			collectionTS = record["collection_ts"].(string)
			continue
		}
		if record["collection_ts"] != collectionTS {
			t.Fatalf("expected shared collection_ts %s, got %+v", collectionTS, record)
		}
	}
}
