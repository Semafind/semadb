package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/lmittmann/tint"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/semafind/semadb/cluster"
	"github.com/semafind/semadb/config"
	"github.com/semafind/semadb/httpapi"
)

// ---------------------------

func setupLogging(cfg config.ConfigMap) {
	logLevel := slog.LevelInfo
	if cfg.Debug {
		logLevel = slog.LevelDebug
	}
	if cfg.PrettyLogOutput {
		// Set global logger with custom options
		slog.SetDefault(slog.New(
			tint.NewTextHandler(os.Stdout, &tint.Options{Level: logLevel, TimeFormat: time.Kitchen}),
		))
	} else {
		slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: logLevel})))
	}
	if cfg.Debug {
		slog.Debug("Configuration", "config", cfg)
	}
	// ---------------------------
	slog.Debug("Debug mode enabled")
}

// ---------------------------

func main() {
	cfg, err := config.LoadConfig()
	if err != nil {
		slog.Error("Failed to load configuration", "error", err)
		os.Exit(1)
	}
	// ---------------------------
	setupLogging(cfg)
	// ---------------------------
	slog.Info("Detected CPU count", "cpu_count", runtime.NumCPU())
	// ---------------------------
	reg := prometheus.NewRegistry()
	// reg.MustRegister(collectors.NewGoCollector())
	// ---------------------------
	// Setup cluster state
	clusterNode, err := cluster.NewNode(cfg.ClusterNode)
	if err != nil {
		slog.Error("Failed to create cluster state", "error", err)
		os.Exit(1)
	}
	clusterNode.RegisterMetrics(reg)
	if err := clusterNode.Serve(); err != nil {
		slog.Error("Failed to start cluster node", "error", err)
		os.Exit(1)
	}
	if err := clusterNode.Sync(); err != nil {
		slog.Error("Failed to sync cluster node", "error", err)
		os.Exit(1)
	}
	// ---------------------------
	httpServer := httpapi.RunHTTPServer(clusterNode, cfg.HttpApi, reg)
	// ---------------------------
	// Conditional imports are not possible in Go, so we comment this out.
	// import _ "net/http/pprof" and then start the pprof server
	// access using http://localhost:8070/debug/pprof/
	// Useful commands:
	// go tool pprof -http=:8000 http://localhost:8071/debug/pprof/profile?seconds=20
	// go tool pprof -http=:8000 http://localhost:8071/debug/pprof/heap
	// ---------------------------
	// go func() {
	// 	debugPort := cfg.HttpApi.HttpPort - 10
	// 	err := http.ListenAndServe(":"+strconv.Itoa(debugPort), nil)
	// 	slog.Info("pprof", "error", err)
	// }()
	// ---------------------------
	quit := make(chan os.Signal, 1)
	// kill (no param) default send syscanll.SIGTERM
	// kill -2 is syscall.SIGINT
	// kill -9 is syscall. SIGKILL but can"t be catch, so don't need add it
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)
	sig := <-quit
	// ---------------------------
	slog.Info("Shutting down server...", "signal", sig.String())
	// The default kubernetes grace period is 30 seconds
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	if err := httpServer.Shutdown(ctx); err != nil {
		slog.Error("HTTP server forced to shut", "error", err)
	}
	cancel()
	// ---------------------------
	clusterNode.Close()
	// ---------------------------
}
