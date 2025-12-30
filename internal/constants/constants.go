// Package constants defines default configuration values and limits used across the application.
// These values have been carefully chosen based on PostgreSQL best practices and testing.
// Modifying these values should be done with caution and thorough testing.
package constants

import "time"

const (
	// DefaultParallel is the default number of parallel sync workers.
	// Range: 1-32. Higher values may cause connection pool exhaustion.
	DefaultParallel = 4

	// MaxParallel is the maximum allowed parallel workers to prevent resource exhaustion.
	MaxParallel = 32

	// MinParallel is the minimum parallel workers (sequential processing).
	MinParallel = 1

	// DefaultBatchSize is the default number of rows to process per batch.
	// Range: 100-10000. Larger batches improve throughput but use more memory.
	DefaultBatchSize = 1000

	// MaxBatchSize is the maximum batch size to prevent memory exhaustion.
	MaxBatchSize = 10000

	// MinBatchSize is the minimum batch size for meaningful batching.
	MinBatchSize = 100

	// SmallTableThreshold defines the maximum row count for a table to be considered "small"
	// and eligible for full-table sync without a timestamp column.
	// Tables above this threshold require a timestamp column for incremental sync.
	SmallTableThreshold = 1000

	// DefaultTimestampColumn is the default column name used for incremental sync.
	// Must be a timestamp/timestamptz column that is updated on row modification.
	DefaultTimestampColumn = "updated_at"

	// DefaultSchema is the default PostgreSQL schema to sync.
	DefaultSchema = "public"

	// DefaultConnMaxLifetime is the default maximum lifetime of database connections.
	// Prevents stale connections and helps with load balancer compatibility.
	DefaultConnMaxLifetime = 5 * time.Minute

	// MaxConnMaxLifetime is the maximum connection lifetime to prevent resource leaks.
	MaxConnMaxLifetime = 30 * time.Minute

	// DefaultMaxIdleConnsMultiplier is calculated as a multiplier of Parallel workers.
	DefaultMaxIdleConnsMultiplier = 1

	// DefaultMaxOpenConnsMultiplier is calculated as a multiplier of Parallel workers.
	DefaultMaxOpenConnsMultiplier = 2

	// MaxColumnNameLength is the maximum length for a PostgreSQL identifier.
	MaxColumnNameLength = 63

	// MaxTableNameLength is the maximum length for a PostgreSQL table name.
	MaxTableNameLength = 63
)
