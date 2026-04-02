// Package main provides the entry point for the pgsync CLI application.
// pgsync is a PostgreSQL table synchronization tool supporting incremental
// and full sync modes with parallel processing.
//
// Exit Codes:
//   - 0: Success
//   - 1: Error (configuration, connection, sync failure)
//   - 130: Interrupted by user (SIGINT)
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"runtime/debug"
	"strings"
	"syscall"

	"github.com/koltyakov/pgsync/internal/config"
	"github.com/koltyakov/pgsync/internal/constants"
	"github.com/koltyakov/pgsync/internal/server"
	"github.com/koltyakov/pgsync/internal/sync"
)

// Exit codes following Unix conventions
const (
	exitSuccess     = 0
	exitError       = 1
	exitInterrupted = 130 // 128 + SIGINT(2)
)

func main() {
	exitCode := func() (code int) {
		defer func() {
			if r := recover(); r != nil {
				fmt.Fprintf(os.Stderr, "FATAL: Unrecovered panic: %v\n", r) //nolint:gosec // G705 - stderr is not user-facing HTML
				fmt.Fprintf(os.Stderr, "Stack trace:\n%s\n", debug.Stack())
				code = exitError
			}
		}()
		return run()
	}()
	os.Exit(exitCode)
}

// run contains the main application logic, separated for testability.
// Returns exit code.
func run() int {
	// Set up structured logging
	logLevel := new(slog.LevelVar)
	logLevel.Set(slog.LevelInfo)
	handler := slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: logLevel,
	})
	logger := slog.New(handler)
	slog.SetDefault(logger)

	var (
		sourceDB   = flag.String("source", "", "Source database connection string (required)")
		targetDB   = flag.String("target", "", "Target database connection string (required)")
		schema     = flag.String("schema", constants.DefaultSchema, "Schema to sync")
		include    = flag.String("include", "", "Comma-separated list of tables to include (supports wildcards)")
		exclude    = flag.String("exclude", "", "Comma-separated list of tables to exclude (supports wildcards)")
		timestamp  = flag.String("timestamp", constants.DefaultTimestampColumn, "Timestamp column name for incremental sync")
		parallel   = flag.Int("parallel", constants.DefaultParallel, fmt.Sprintf("Number of parallel sync sessions (1-%d)", constants.MaxParallel))
		batchSize  = flag.Int("batch-size", constants.DefaultBatchSize, fmt.Sprintf("Batch size for data processing (%d-%d)", constants.MinBatchSize, constants.MaxBatchSize))
		verbose    = flag.Bool("verbose", false, "Enable verbose logging")
		integrity  = flag.Bool("integrity", false, "Run post-sync integrity checks and write integrity.csv")
		dryRun     = flag.Bool("dry-run", false, "Preview sync operations without making changes")
		reconcile  = flag.Bool("reconcile", false, "Full reconciliation mode: compare all rows by primary key, sync missing/different rows")
		configFile = flag.String("config", "", "Path to configuration file")
		serverMode = flag.Bool("server", false, "Start web UI server instead of running sync")
		serverPort = flag.Int("port", 8080, "Port for web UI server (only used with -server)")
	)

	flag.Parse()

	cfg := &config.Config{
		SourceDB:     *sourceDB,
		TargetDB:     *targetDB,
		Schema:       *schema,
		TimestampCol: *timestamp,
		Parallel:     *parallel,
		BatchSize:    *batchSize,
		Verbose:      *verbose,
		Integrity:    *integrity,
		DryRun:       *dryRun,
		Reconcile:    *reconcile,
	}

	if *include != "" {
		cfg.IncludeTables = strings.Split(*include, ",")
	}
	if *exclude != "" {
		cfg.ExcludeTables = strings.Split(*exclude, ",")
	}

	if *configFile != "" {
		if err := config.LoadFromFile(*configFile, cfg); err != nil {
			slog.Error("Failed to load config file", "error", err)
			os.Exit(1)
		}
	}

	// Server mode - start web UI
	if *serverMode {
		if cfg.SourceDB == "" || cfg.TargetDB == "" {
			fmt.Fprintf(os.Stderr, "Server mode requires -source and -target database connection strings\n")
			fmt.Fprintf(os.Stderr, "Usage: %s -server -source <source_db> -target <target_db> [-port 8080]\n", os.Args[0]) //nolint:gosec // G705 - stderr output
			return exitError
		}

		// Validate port range
		if *serverPort < 1 || *serverPort > 65535 {
			fmt.Fprintf(os.Stderr, "Invalid port: %d (must be 1-65535)\n", *serverPort)
			return exitError
		}

		// Set up graceful shutdown with context cancellation
		ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer cancel()

		srv := server.New(&server.Config{
			Port:     *serverPort,
			SourceDB: cfg.SourceDB,
			TargetDB: cfg.TargetDB,
			Schema:   cfg.Schema,
		})

		slog.Info("Starting web UI server", "port", *serverPort)

		if err := srv.Start(ctx); err != nil {
			if ctx.Err() != nil {
				slog.Info("Server shutdown completed")
				return exitSuccess
			}
			slog.Error("Server error", "error", err)
			return exitError
		}
		return exitSuccess
	}

	// Validate and apply defaults
	if err := cfg.Validate(); err != nil {
		fmt.Fprintf(os.Stderr, "Configuration error: %v\n", err)
		fmt.Fprintf(os.Stderr, "Usage: %s -source <source_db> -target <target_db> [options]\n", os.Args[0]) //nolint:gosec // G705 - stderr output
		fmt.Fprintf(os.Stderr, "Or use: %s -config <config_file>\n", os.Args[0])                            //nolint:gosec // G705 - stderr output
		flag.PrintDefaults()
		return exitError
	}

	// Adjust log level for verbose mode
	if cfg.Verbose {
		logLevel.Set(slog.LevelDebug)
	}

	if cfg.DryRun {
		slog.Info("Running in dry-run mode - no changes will be made")
	}

	// Set up graceful shutdown with context cancellation
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	syncer, err := sync.New(cfg)
	if err != nil {
		slog.Error("Failed to create syncer", "error", err)
		return exitError
	}
	defer func() {
		if closeErr := syncer.Close(); closeErr != nil {
			slog.Warn("Error closing syncer", "error", closeErr)
		}
	}()

	if err := syncer.Sync(ctx); err != nil {
		if ctx.Err() != nil {
			slog.Info("Sync interrupted by user")
			return exitInterrupted
		}
		slog.Error("Sync failed", "error", err)
		return exitError
	}

	// Print summary
	stats := syncer.GetStats()
	slog.Info("Sync completed successfully",
		"totalUpserts", stats.TotalUpserts,
		"totalDeletes", stats.TotalDeletes,
		"skippedTables", len(stats.SkippedTables))

	return exitSuccess
}
