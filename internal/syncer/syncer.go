package syncer

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strings"
	"sync"

	"github.com/koltyakov/pgsync/internal/config"
	"github.com/koltyakov/pgsync/internal/db"
	_ "github.com/lib/pq" // PostgreSQL driver registration
)

// Syncer handles the synchronization between two PostgreSQL databases
type Syncer struct {
	cfg            *config.Config
	sourceDB       *sql.DB
	targetDB       *sql.DB
	inspector      *db.Inspector
	mu             sync.Mutex
	upsertsByTable map[string]int64
	deletesByTable map[string]int64
	totalUpserts   int64
	totalDeletes   int64
	skippedTables  []string
	logger         *slog.Logger
	progress       ProgressHandler
}

// New creates a new Syncer instance.
// The caller is responsible for calling Close() to release database connections.
func New(cfg *config.Config) (*Syncer, error) {
	// Defensive: validate config is not nil
	if cfg == nil {
		return nil, fmt.Errorf("configuration is nil: cannot create syncer without configuration")
	}

	// Ensure config is validated (applies defaults)
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	logger := slog.Default()

	sourceDB, err := sql.Open("postgres", cfg.SourceDB)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to source database: %w", err)
	}

	// Configure connection pool for source DB
	sourceDB.SetMaxOpenConns(cfg.MaxOpenConns)
	sourceDB.SetMaxIdleConns(cfg.MaxIdleConns)
	sourceDB.SetConnMaxLifetime(cfg.ConnMaxLifetime)

	targetDB, err := sql.Open("postgres", cfg.TargetDB)
	if err != nil {
		_ = sourceDB.Close()
		return nil, fmt.Errorf("failed to connect to target database: %w", err)
	}

	// Configure connection pool for target DB
	targetDB.SetMaxOpenConns(cfg.MaxOpenConns)
	targetDB.SetMaxIdleConns(cfg.MaxIdleConns)
	targetDB.SetConnMaxLifetime(cfg.ConnMaxLifetime)

	ctx := context.Background()

	if err := retry(ctx, func() error { return sourceDB.Ping() }); err != nil {
		_ = sourceDB.Close()
		_ = targetDB.Close()
		return nil, fmt.Errorf("failed to ping source database: %w", err)
	}

	if err := retry(ctx, func() error { return targetDB.Ping() }); err != nil {
		_ = sourceDB.Close()
		_ = targetDB.Close()
		return nil, fmt.Errorf("failed to ping target database: %w", err)
	}

	inspector := db.NewInspector(sourceDB, targetDB, cfg.Schema)

	logger.Debug("Database connections established",
		"maxOpenConns", cfg.MaxOpenConns,
		"maxIdleConns", cfg.MaxIdleConns,
		"connMaxLifetime", cfg.ConnMaxLifetime,
	)

	return &Syncer{
		cfg:            cfg,
		sourceDB:       sourceDB,
		targetDB:       targetDB,
		inspector:      inspector,
		upsertsByTable: make(map[string]int64),
		deletesByTable: make(map[string]int64),
		skippedTables:  make([]string, 0),
		logger:         logger,
		progress:       &noopProgressHandler{},
	}, nil
}

// NewWithProgress creates a new Syncer instance with progress reporting
func NewWithProgress(cfg *config.Config, progressHandler ProgressHandler) (*Syncer, error) {
	syncer, err := New(cfg)
	if err != nil {
		return nil, err
	}
	if progressHandler != nil {
		syncer.progress = progressHandler
	}
	return syncer, nil
}

// GetStats returns current sync statistics.
// Safe to call concurrently from multiple goroutines.
// Returns a copy of the stats to prevent data races.
func (s *Syncer) GetStats() Stats {
	if s == nil {
		return Stats{
			UpsertsByTable: make(map[string]int64),
			DeletesByTable: make(map[string]int64),
			SkippedTables:  []string{},
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	return Stats{
		TotalUpserts:   s.totalUpserts,
		TotalDeletes:   s.totalDeletes,
		UpsertsByTable: copyMap(s.upsertsByTable),
		DeletesByTable: copyMap(s.deletesByTable),
		SkippedTables:  append([]string(nil), s.skippedTables...),
	}
}

func copyMap(m map[string]int64) map[string]int64 {
	result := make(map[string]int64, len(m))
	for k, v := range m {
		result[k] = v
	}
	return result
}

// Close closes all database connections.
// Safe to call multiple times; subsequent calls are no-ops.
// Returns an error if any connection fails to close, but attempts to close all.
func (s *Syncer) Close() error {
	if s == nil {
		return nil
	}

	var errs []string

	if s.sourceDB != nil {
		if err := s.sourceDB.Close(); err != nil {
			errs = append(errs, fmt.Sprintf("source DB: %v", err))
		}
	}
	if s.targetDB != nil {
		if err := s.targetDB.Close(); err != nil {
			errs = append(errs, fmt.Sprintf("target DB: %v", err))
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("errors closing databases: %s", strings.Join(errs, ", "))
	}
	return nil
}

// tableCounts returns current counts for a table
func (s *Syncer) tableCounts(table string) (int64, int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.upsertsByTable[table], s.deletesByTable[table]
}

func (s *Syncer) addUpserts(table string, n int) {
	if n <= 0 {
		return
	}
	s.mu.Lock()
	s.upsertsByTable[table] += int64(n)
	s.totalUpserts += int64(n)
	s.mu.Unlock()
}

func (s *Syncer) addDeletes(table string, n int) {
	if n <= 0 {
		return
	}
	s.mu.Lock()
	s.deletesByTable[table] += int64(n)
	s.totalDeletes += int64(n)
	s.mu.Unlock()
}

func (s *Syncer) addSkipped(table string) {
	s.mu.Lock()
	s.skippedTables = append(s.skippedTables, table)
	s.mu.Unlock()
}
