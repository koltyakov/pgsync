package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/koltyakov/pgsync/internal/config"
	"github.com/koltyakov/pgsync/internal/sync"
)

func main() {
	// Set log format to show only time without date
	log.SetFlags(log.Ltime)

	var (
		sourceDB   = flag.String("source", "", "Source database connection string")
		targetDB   = flag.String("target", "", "Target database connection string")
		schema     = flag.String("schema", "public", "Schema to sync")
		include    = flag.String("include", "", "Comma-separated list of tables to include")
		exclude    = flag.String("exclude", "", "Comma-separated list of tables to exclude")
		timestamp  = flag.String("timestamp", "updated_at", "Timestamp column name for incremental sync")
		parallel   = flag.Int("parallel", 4, "Number of parallel sync sessions")
		batchSize  = flag.Int("batch-size", 1000, "Batch size for data processing")
		dryRun     = flag.Bool("dry-run", false, "Perform a dry run without actual data changes")
		verbose    = flag.Bool("verbose", false, "Enable verbose logging")
		configFile = flag.String("config", "", "Path to configuration file")
		stateDB    = flag.String("state-db", "./pgsync.db", "SQLite database path for state management")
	)

	flag.Parse()

	cfg := &config.Config{
		SourceDB:     *sourceDB,
		TargetDB:     *targetDB,
		Schema:       *schema,
		TimestampCol: *timestamp,
		Parallel:     *parallel,
		BatchSize:    *batchSize,
		DryRun:       *dryRun,
		Verbose:      *verbose,
		StateDB:      *stateDB,
	}

	if *include != "" {
		cfg.IncludeTables = strings.Split(*include, ",")
	}
	if *exclude != "" {
		cfg.ExcludeTables = strings.Split(*exclude, ",")
	}

	if *configFile != "" {
		if err := config.LoadFromFile(*configFile, cfg); err != nil {
			log.Fatalf("Failed to load config file: %v", err)
		}
	}

	// Validate required parameters after config loading
	if cfg.SourceDB == "" || cfg.TargetDB == "" {
		fmt.Fprintf(os.Stderr, "Usage: %s -source <source_db> -target <target_db> [options]\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Or use: %s -config <config_file>\n", os.Args[0])
		flag.PrintDefaults()
		os.Exit(1)
	}

	syncer, err := sync.New(cfg)
	if err != nil {
		log.Fatalf("Failed to create syncer: %v", err)
	}
	defer syncer.Close()

	if err := syncer.Sync(); err != nil {
		log.Fatalf("Sync failed: %v", err)
	}

	fmt.Println("Sync completed successfully")
}
