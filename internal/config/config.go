package config

import (
	"encoding/json"
	"os"
)

// Config holds the application configuration
type Config struct {
	SourceDB      string   `json:"sourceDb"`
	TargetDB      string   `json:"targetDb"`
	Schema        string   `json:"schema"`
	IncludeTables []string `json:"includeTables"`
	ExcludeTables []string `json:"excludeTables"`
	TimestampCol  string   `json:"timestampColumn"`
	Parallel      int      `json:"parallel"`
	BatchSize     int      `json:"batchSize"`
	Verbose       bool     `json:"verbose"`
	Integrity     bool     `json:"integrity"`
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
