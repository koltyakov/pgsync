package config

import (
	"encoding/json"
	"os"
)

// Config holds the application configuration
type Config struct {
	SourceDB      string   `json:"source_db"`
	TargetDB      string   `json:"target_db"`
	Schema        string   `json:"schema"`
	IncludeTables []string `json:"include_tables"`
	ExcludeTables []string `json:"exclude_tables"`
	TimestampCol  string   `json:"timestamp_column"`
	Parallel      int      `json:"parallel"`
	BatchSize     int      `json:"batch_size"`
	DryRun        bool     `json:"dry_run"`
	Verbose       bool     `json:"verbose"`
	StateDB       string   `json:"state_db"`
}

// LoadFromFile loads configuration from a JSON file
func LoadFromFile(path string, cfg *Config) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}

	return json.Unmarshal(data, cfg)
}

// SaveToFile saves configuration to a JSON file
func (c *Config) SaveToFile(path string) error {
	data, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(path, data, 0644)
}
