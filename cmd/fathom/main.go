package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"fathom/internal/collector"
	"fathom/internal/config"
	"fathom/internal/logger"
)

func main() {
	configPath := flag.String("config", "configs/config.yaml", "Path to YAML configuration file")
	flag.StringVar(configPath, "c", "configs/config.yaml", "Path to YAML configuration file (shorthand)")
	flag.Parse()

	// Load configuration
	cfg, err := config.Load(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error loading configuration: %v\n", err)
		os.Exit(1)
	}

	// Initialize logger
	l := logger.Init(cfg.Agent.LogLevel)

	// Create context that cancels on SIGINT or SIGTERM for graceful shutdown
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Log static host metadata snapshot once at startup
	hostMeta := collector.GetHostMetadata()
	hostName := hostNameFromMetadata(hostMeta)

	l.Info("agent_start",
		slog.String("component", "agent"),
		slog.String("host", hostName),
		slog.String("version", cfg.Agent.Version),
	)

	l.LogAttrs(ctx, slog.LevelInfo, hostMeta.Event,
		slog.String("component", hostMeta.Component),
		slog.String("host", hostName),
		slog.Any("data", hostMeta.Data),
	)

	// Set up signal channel to handle SIGHUP configuration hot reloading
	sighupChan := make(chan os.Signal, 1)
	signal.Notify(sighupChan, syscall.SIGHUP)

	// Thread-safe configuration manager
	var (
		cfgMu     sync.RWMutex
		activeCfg = cfg
	)

	// Initialize system metrics/metadata collectors
	diskCol := collector.NewDiskCollector(&cfg.Disk)
	netCol := collector.NewNetworkCollector(&cfg.Network)

	collectors := []collector.Collector{
		collector.NewCPUCollector(),
		collector.NewMemoryCollector(),
		diskCol,
		netCol,
		collector.NewSystemCollector(),
		collector.NewDiskIOCollector(),
	}
	collectorFailures := make(map[string]bool)

	// Channel to communicate collection interval updates on reload
	intervalUpdateChan := make(chan time.Duration, 1)

	// Goroutine for handling configuration hot reloads
	go func() {
		for {
			select {
			case <-sighupChan:
				l.Info("config_reload_triggered", slog.String("component", "agent"))
				newCfg, err := config.Load(*configPath)
				if err != nil {
					l.Error("config_reload_failed",
						slog.String("component", "agent"),
						slog.String("error", err.Error()),
					)
					continue
				}

				cfgMu.Lock()
				activeCfg = newCfg
				cfgMu.Unlock()

				logger.SetLevel(newCfg.Agent.LogLevel)
				diskCol.UpdateConfig(&newCfg.Disk)
				netCol.UpdateConfig(&newCfg.Network)
				l.Info("config_reload_success",
					slog.String("component", "agent"),
					slog.String("version", newCfg.Agent.Version),
				)

				// Send updated interval to metrics loop
				newDur, _ := time.ParseDuration(newCfg.Agent.CollectInterval)
				select {
				case intervalUpdateChan <- newDur:
				default:
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	// Start system metrics collection loop
	go func() {
		cfgMu.RLock()
		initIntervalStr := activeCfg.Agent.CollectInterval
		cfgMu.RUnlock()

		dur, _ := time.ParseDuration(initIntervalStr)

		// Run initial collection (which triggers metadata snapshots)
		runCollection(ctx, l, collectors, collectorFailures, hostName)

		ticker := time.NewTicker(dur)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				runCollection(ctx, l, collectors, collectorFailures, hostName)
			case newDur := <-intervalUpdateChan:
				ticker.Reset(newDur)
			case <-ctx.Done():
				return
			}
		}
	}()

	// Block until SIGINT or SIGTERM is received
	<-ctx.Done()

	// Log agent stop event
	l.Info("agent_stop", slog.String("component", "agent"), slog.String("host", hostName))
}

func hostNameFromMetadata(hostMeta collector.Event) string {
	if hostname, ok := hostMeta.Data["hostname"].(string); ok && hostname != "" {
		return hostname
	}
	return "unknown"
}

// runCollection iterates over all collectors and logs their returned events.
func runCollection(ctx context.Context, l *slog.Logger, collectors []collector.Collector, failures map[string]bool, host string) {
	collectionTS := time.Now().Format(time.RFC3339Nano)
	for _, col := range collectors {
		events, err := col.Collect(ctx)
		if err != nil {
			action := "fail_collect"
			level := slog.LevelError
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				action = "cancel_collect"
				level = slog.LevelDebug
			}
			failures[col.Name()] = true
			l.LogAttrs(ctx, level, "collector_error",
				slog.String("component", "collector"),
				slog.String("host", host),
				slog.String("collection_ts", collectionTS),
				slog.String("collector", col.Name()),
				slog.String("action", action),
				slog.String("error", err.Error()),
			)
			continue
		}
		if failures[col.Name()] {
			l.Info("collector_recovered",
				slog.String("component", "collector"),
				slog.String("host", host),
				slog.String("collection_ts", collectionTS),
				slog.String("collector", col.Name()),
				slog.String("action", "recover_collect"),
			)
			failures[col.Name()] = false
		}

		for _, ev := range events {
			attrs := []slog.Attr{
				slog.String("host", host),
				slog.String("collection_ts", collectionTS),
				slog.String("collector", ev.Collector),
			}
			if ev.Component != "" {
				attrs = append(attrs, slog.String("component", ev.Component))
			}
			attrs = append(attrs, slog.Any("data", ev.Data))

			l.LogAttrs(ctx, slog.LevelInfo, ev.Event, attrs...)
		}
	}
}
