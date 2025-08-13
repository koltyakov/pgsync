package config

import (
	"os"
	"reflect"
	"testing"
)

func TestLoadFromFile(t *testing.T) {
	// Create a temporary config file
	configContent := `{
		"source_db": "postgres://test:test@localhost:5432/source",
		"target_db": "postgres://test:test@localhost:5432/target",
		"schema": "test_schema",
		"timestamp_column": "modified_at",
		"parallel": 8,
		"batch_size": 2000,
		"include_tables": ["table1", "table2"],
		"exclude_tables": ["table3"],
		"verbose": true
	}`

	tmpFile, err := os.CreateTemp("", "test_config_*.json")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	if _, err := tmpFile.WriteString(configContent); err != nil {
		t.Fatalf("Failed to write to temp file: %v", err)
	}
	tmpFile.Close()

	// Test loading config
	cfg := &Config{}
	err = LoadFromFile(tmpFile.Name(), cfg)
	if err != nil {
		t.Fatalf("Failed to load config: %v", err)
	}

	// Verify loaded values
	expected := &Config{
		SourceDB:      "postgres://test:test@localhost:5432/source",
		TargetDB:      "postgres://test:test@localhost:5432/target",
		Schema:        "test_schema",
		TimestampCol:  "modified_at",
		Parallel:      8,
		BatchSize:     2000,
		IncludeTables: []string{"table1", "table2"},
		ExcludeTables: []string{"table3"},
		Verbose:       true,
	}

	if !reflect.DeepEqual(cfg, expected) {
		t.Errorf("Config mismatch.\nExpected: %+v\nGot: %+v", expected, cfg)
	}
}

func TestSaveToFile(t *testing.T) {
	cfg := &Config{
		SourceDB:      "postgres://test:test@localhost:5432/source",
		TargetDB:      "postgres://test:test@localhost:5432/target",
		Schema:        "public",
		TimestampCol:  "updated_at",
		Parallel:      4,
		BatchSize:     1000,
		IncludeTables: []string{"users", "orders"},
		ExcludeTables: []string{"logs"},
		Verbose:       false,
		StateDB:       "./test.db",
	}

	tmpFile, err := os.CreateTemp("", "test_save_config_*.json")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	tmpFile.Close()

	// Save config
	err = cfg.SaveToFile(tmpFile.Name())
	if err != nil {
		t.Fatalf("Failed to save config: %v", err)
	}

	// Load it back and verify
	loadedCfg := &Config{}
	err = LoadFromFile(tmpFile.Name(), loadedCfg)
	if err != nil {
		t.Fatalf("Failed to load saved config: %v", err)
	}

	if !reflect.DeepEqual(cfg, loadedCfg) {
		t.Errorf("Config mismatch after save/load.\nOriginal: %+v\nLoaded: %+v", cfg, loadedCfg)
	}
}
