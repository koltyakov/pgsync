// Package config handles application configuration loading, validation, and persistence.
package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/koltyakov/pgsync/internal/constants"
)

// Config holds the application configuration
type Config struct {
	SourceDB       string              `json:"sourceDb"`
	TargetDB       string              `json:"targetDb"`
	Schema         string              `json:"schema"`
	IncludeTables  []string            `json:"includeTables"`
	ExcludeTables  []string            `json:"excludeTables"`
	IncludeColumns map[string][]string `json:"includeColumns"`
	TimestampCol   string              `json:"timestampColumn"`
	Parallel       int                 `json:"parallel"`
	BatchSize      int                 `json:"batchSize"`
	Verbose        bool                `json:"verbose"`
	Integrity      bool                `json:"integrity"`
	IntegrityPath  string              `json:"integrityPath,omitempty"`
	DryRun         bool                `json:"dryRun"`
	Reconcile      bool                `json:"reconcile"`

	// Connection pool settings (optional, defaults are computed from Parallel)
	MaxOpenConns    int           `json:"maxOpenConns,omitempty"`
	MaxIdleConns    int           `json:"maxIdleConns,omitempty"`
	ConnMaxLifetime time.Duration `json:"connMaxLifetime,omitempty"`
}

// Validate checks the configuration for errors and applies defaults.
// Returns detailed error messages to help users fix configuration issues.
func (c *Config) Validate() error {
	// Required fields
	if c.SourceDB == "" {
		return errors.New("sourceDb is required: specify the source PostgreSQL connection string")
	}
	if c.TargetDB == "" {
		return errors.New("targetDb is required: specify the target PostgreSQL connection string")
	}

	// Prevent accidental self-sync which could cause data corruption
	if c.SourceDB == c.TargetDB {
		return errors.New("sourceDb and targetDb cannot be the same: this would cause data corruption")
	}

	// Apply defaults for missing values
	if c.Schema == "" {
		c.Schema = constants.DefaultSchema
	}
	if c.TimestampCol == "" {
		c.TimestampCol = constants.DefaultTimestampColumn
	}

	// Validate and clamp Parallel to safe bounds
	if c.Parallel <= 0 {
		c.Parallel = constants.DefaultParallel
	} else if c.Parallel > constants.MaxParallel {
		return fmt.Errorf("parallel value %d exceeds maximum %d: reduce parallel workers to prevent resource exhaustion",
			c.Parallel, constants.MaxParallel)
	}

	// Validate and clamp BatchSize to safe bounds
	switch {
	case c.BatchSize <= 0:
		c.BatchSize = constants.DefaultBatchSize
	case c.BatchSize > constants.MaxBatchSize:
		return fmt.Errorf("batchSize value %d exceeds maximum %d: reduce batch size to prevent memory exhaustion",
			c.BatchSize, constants.MaxBatchSize)
	case c.BatchSize < constants.MinBatchSize:
		c.BatchSize = constants.MinBatchSize // Silently upgrade tiny batches
	}

	// Connection pool defaults (computed from Parallel if not explicitly set)
	if c.MaxOpenConns <= 0 {
		c.MaxOpenConns = c.Parallel * constants.DefaultMaxOpenConnsMultiplier
	}
	if c.MaxIdleConns <= 0 {
		c.MaxIdleConns = c.Parallel * constants.DefaultMaxIdleConnsMultiplier
	}
	if c.ConnMaxLifetime <= 0 {
		c.ConnMaxLifetime = constants.DefaultConnMaxLifetime
	} else if c.ConnMaxLifetime > constants.MaxConnMaxLifetime {
		c.ConnMaxLifetime = constants.MaxConnMaxLifetime
	}

	// Validate schema name length (PostgreSQL limit)
	if len(c.Schema) > constants.MaxTableNameLength {
		return fmt.Errorf("schema name exceeds PostgreSQL limit of %d characters", constants.MaxTableNameLength)
	}

	// Validate timestamp column name
	if len(c.TimestampCol) > constants.MaxColumnNameLength {
		return fmt.Errorf("timestamp column name exceeds PostgreSQL limit of %d characters", constants.MaxColumnNameLength)
	}

	return nil
}

// LoadFromFile loads configuration from a JSON file
func LoadFromFile(path string, cfg *Config) error {
	data, err := os.ReadFile(path) //nolint:gosec // G304 - path is user-provided configuration file
	if err != nil {
		return fmt.Errorf("failed to read config file: %w", err)
	}

	if err := json.Unmarshal(data, cfg); err != nil {
		return fmt.Errorf("failed to parse config file: %w", err)
	}

	return nil
}

// SaveToFile saves configuration to a JSON file
func (c *Config) SaveToFile(path string) error {
	data, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(path, data, 0600) //nolint:gosec // G703 - path is user-provided
}
