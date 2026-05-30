package main

import (
	"context"
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

	l.Info("agent_start",
		slog.String("component", "agent"),
		slog.String("version", cfg.Agent.Version),
	)

	// Create context that cancels on SIGINT or SIGTERM for graceful shutdown
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Log static host metadata snapshot once at startup
	hostMeta := collector.GetHostMetadata()
	l.LogAttrs(ctx, slog.LevelInfo, hostMeta.Event,
		slog.String("component", hostMeta.Component),
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
	}

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
		runCollection(ctx, l, collectors)

		ticker := time.NewTicker(dur)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				runCollection(ctx, l, collectors)
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
	l.Info("agent_stop", slog.String("component", "agent"))
}

// runCollection iterates over all collectors and logs their returned events.
func runCollection(ctx context.Context, l *slog.Logger, collectors []collector.Collector) {
	for _, col := range collectors {
		events, err := col.Collect(ctx)
		if err != nil {
			l.Error("collector_error",
				slog.String("component", "collector"),
				slog.String("collector", col.Name()),
				slog.String("error", err.Error()),
			)
			continue
		}

		for _, ev := range events {
			attrs := []slog.Attr{
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
