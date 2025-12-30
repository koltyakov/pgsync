// Package constants defines default configuration values and limits used across the application.
package constants

import "time"

const (
	// DefaultParallel is the default number of parallel sync workers.
	DefaultParallel = 4

	// DefaultBatchSize is the default number of rows to process per batch.
	DefaultBatchSize = 1000

	// SmallTableThreshold defines the maximum row count for a table to be considered "small"
	// and eligible for full-table sync without a timestamp column.
	SmallTableThreshold = 1000

	// DefaultTimestampColumn is the default column name used for incremental sync.
	DefaultTimestampColumn = "updated_at"

	// DefaultSchema is the default PostgreSQL schema to sync.
	DefaultSchema = "public"

	// DefaultConnMaxLifetime is the default maximum lifetime of database connections.
	DefaultConnMaxLifetime = 5 * time.Minute

	// DefaultMaxIdleConnsMultiplier is calculated as a multiplier of Parallel workers.
	DefaultMaxIdleConnsMultiplier = 1

	// DefaultMaxOpenConnsMultiplier is calculated as a multiplier of Parallel workers.
	DefaultMaxOpenConnsMultiplier = 2
)
