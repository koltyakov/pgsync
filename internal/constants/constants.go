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
	// Range: 100-50000. Larger batches improve throughput but use more memory.
	// 5000 provides good balance for most workloads.
	DefaultBatchSize = 5000

	// MaxBatchSize is the maximum batch size to prevent memory exhaustion.
	// 50000 rows is safe for most systems and significantly improves throughput.
	MaxBatchSize = 50000

	// MinBatchSize is the minimum batch size for meaningful batching.
	MinBatchSize = 100

	// SmallTableThreshold defines the maximum row count for a table to be considered "small"
	// and eligible for full-table sync without a timestamp column.
	// Tables above this threshold require a timestamp column for incremental sync.
	// 5000 rows provides good balance - full comparison is efficient at this scale
	// and avoids requiring timestamp columns for medium-sized lookup tables.
	SmallTableThreshold = 5000

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
	// Higher idle conn count reduces connection setup overhead for bursty workloads.
	DefaultMaxIdleConnsMultiplier = 2

	// DefaultMaxOpenConnsMultiplier is calculated as a multiplier of Parallel workers.
	// Allows each worker to have multiple concurrent operations (fetch + upsert).
	DefaultMaxOpenConnsMultiplier = 3

	// MaxColumnNameLength is the maximum length for a PostgreSQL identifier.
	MaxColumnNameLength = 63

	// MaxTableNameLength is the maximum length for a PostgreSQL table name.
	MaxTableNameLength = 63
)
