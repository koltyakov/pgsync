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
	IncludeColumns map[string][]string `json:"includeColumns"` // Table -> columns to sync (empty = all)
	TimestampCol   string              `json:"timestampColumn"`
	Parallel       int                 `json:"parallel"`
	BatchSize      int                 `json:"batchSize"`
	Verbose        bool                `json:"verbose"`
	Integrity      bool                `json:"integrity"`
	DryRun         bool                `json:"dryRun"`
	Reconcile      bool                `json:"reconcile"` // Force full comparison by primary key, ignore timestamps

	// Connection pool settings (optional, defaults are computed from Parallel)
	MaxOpenConns    int           `json:"maxOpenConns,omitempty"`
	MaxIdleConns    int           `json:"maxIdleConns,omitempty"`
	ConnMaxLifetime time.Duration `json:"connMaxLifetime,omitempty"`
}

// Validate checks the configuration for errors and applies defaults.
func (c *Config) Validate() error {
	if c.SourceDB == "" {
		return errors.New("sourceDb is required")
	}
	if c.TargetDB == "" {
		return errors.New("targetDb is required")
	}

	// Apply defaults for missing values
	if c.Schema == "" {
		c.Schema = constants.DefaultSchema
	}
	if c.TimestampCol == "" {
		c.TimestampCol = constants.DefaultTimestampColumn
	}
	if c.Parallel <= 0 {
		c.Parallel = constants.DefaultParallel
	}
	if c.BatchSize <= 0 {
		c.BatchSize = constants.DefaultBatchSize
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
	}

	return nil
}

// LoadFromFile loads configuration from a JSON file
func LoadFromFile(path string, cfg *Config) error {
	data, err := os.ReadFile(path)
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

	return os.WriteFile(path, data, 0644)
}
