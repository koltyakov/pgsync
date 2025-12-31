package config

import (
	"os"
	"reflect"
	"testing"
)

func TestLoadFromFile(t *testing.T) {
	// Create a temporary config file
	configContent := `{
		"sourceDb": "postgres://test:test@localhost:5432/source",
		"targetDb": "postgres://test:test@localhost:5432/target",
		"schema": "test_schema",
		"timestampColumn": "modified_at",
		"parallel": 8,
		"batchSize": 2000,
		"includeTables": ["table1", "table2"],
		"excludeTables": ["table3"],
		"verbose": true
	}`

	tmpFile, err := os.CreateTemp("", "test_config_*.json")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer func() {
		if err := os.Remove(tmpFile.Name()); err != nil {
			t.Logf("Failed to remove temp file: %v", err)
		}
	}()

	if _, err := tmpFile.WriteString(configContent); err != nil {
		t.Fatalf("Failed to write to temp file: %v", err)
	}
	if err := tmpFile.Close(); err != nil {
		t.Fatalf("Failed to close temp file: %v", err)
	}

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
	}

	tmpFile, err := os.CreateTemp("", "test_save_config_*.json")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer func() {
		if err := os.Remove(tmpFile.Name()); err != nil {
			t.Logf("Failed to remove temp file: %v", err)
		}
	}()
	if err := tmpFile.Close(); err != nil {
		t.Fatalf("Failed to close temp file: %v", err)
	}

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

func TestValidate(t *testing.T) {
	tests := []struct {
		name        string
		config      *Config
		expectError bool
		errorMsg    string
	}{
		{
			name: "valid config",
			config: &Config{
				SourceDB: "postgres://localhost/source",
				TargetDB: "postgres://localhost/target",
			},
			expectError: false,
		},
		{
			name: "missing sourceDb",
			config: &Config{
				TargetDB: "postgres://localhost/target",
			},
			expectError: true,
			errorMsg:    "sourceDb is required: specify the source PostgreSQL connection string",
		},
		{
			name: "missing targetDb",
			config: &Config{
				SourceDB: "postgres://localhost/source",
			},
			expectError: true,
			errorMsg:    "targetDb is required: specify the target PostgreSQL connection string",
		},
		{
			name: "same source and target",
			config: &Config{
				SourceDB: "postgres://localhost/same",
				TargetDB: "postgres://localhost/same",
			},
			expectError: true,
			errorMsg:    "sourceDb and targetDb cannot be the same: this would cause data corruption",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()

			if tt.expectError {
				if err == nil {
					t.Error("expected error but got none")
				} else if err.Error() != tt.errorMsg {
					t.Errorf("expected error %q, got %q", tt.errorMsg, err.Error())
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
			}
		})
	}
}

func TestValidateAppliesDefaults(t *testing.T) {
	cfg := &Config{
		SourceDB: "postgres://localhost/source",
		TargetDB: "postgres://localhost/target",
	}

	err := cfg.Validate()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Check defaults were applied
	if cfg.Schema != "public" {
		t.Errorf("expected schema 'public', got %q", cfg.Schema)
	}
	if cfg.TimestampCol != "updated_at" {
		t.Errorf("expected timestampCol 'updated_at', got %q", cfg.TimestampCol)
	}
	if cfg.Parallel != 4 {
		t.Errorf("expected parallel 4, got %d", cfg.Parallel)
	}
	if cfg.BatchSize != 5000 {
		t.Errorf("expected batchSize 5000, got %d", cfg.BatchSize)
	}
	if cfg.MaxOpenConns != 12 { // 4 * 3
		t.Errorf("expected maxOpenConns 12, got %d", cfg.MaxOpenConns)
	}
	if cfg.MaxIdleConns != 8 { // 4 * 2
		t.Errorf("expected maxIdleConns 8, got %d", cfg.MaxIdleConns)
	}
}

func TestValidateRespectsProvidedValues(t *testing.T) {
	cfg := &Config{
		SourceDB:     "postgres://localhost/source",
		TargetDB:     "postgres://localhost/target",
		Schema:       "custom_schema",
		TimestampCol: "modified_at",
		Parallel:     8,
		BatchSize:    5000,
		MaxOpenConns: 20,
		MaxIdleConns: 10,
	}

	err := cfg.Validate()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Check provided values were preserved
	if cfg.Schema != "custom_schema" {
		t.Errorf("expected schema 'custom_schema', got %q", cfg.Schema)
	}
	if cfg.TimestampCol != "modified_at" {
		t.Errorf("expected timestampCol 'modified_at', got %q", cfg.TimestampCol)
	}
	if cfg.Parallel != 8 {
		t.Errorf("expected parallel 8, got %d", cfg.Parallel)
	}
	if cfg.BatchSize != 5000 {
		t.Errorf("expected batchSize 5000, got %d", cfg.BatchSize)
	}
	if cfg.MaxOpenConns != 20 {
		t.Errorf("expected maxOpenConns 20, got %d", cfg.MaxOpenConns)
	}
	if cfg.MaxIdleConns != 10 {
		t.Errorf("expected maxIdleConns 10, got %d", cfg.MaxIdleConns)
	}
}

func TestValidateBoundsChecking(t *testing.T) {
	tests := []struct {
		name      string
		config    *Config
		wantError bool
		errorMsg  string
	}{
		{
			name: "parallel exceeds maximum",
			config: &Config{
				SourceDB: "postgres://localhost/source",
				TargetDB: "postgres://localhost/target",
				Parallel: 100,
			},
			wantError: true,
			errorMsg:  "parallel value 100 exceeds maximum 32: reduce parallel workers to prevent resource exhaustion",
		},
		{
			name: "batchSize exceeds maximum",
			config: &Config{
				SourceDB:  "postgres://localhost/source",
				TargetDB:  "postgres://localhost/target",
				BatchSize: 100000,
			},
			wantError: true,
			errorMsg:  "batchSize value 100000 exceeds maximum 50000: reduce batch size to prevent memory exhaustion",
		},
		{
			name: "small batchSize gets clamped to minimum",
			config: &Config{
				SourceDB:  "postgres://localhost/source",
				TargetDB:  "postgres://localhost/target",
				BatchSize: 10,
			},
			wantError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if tt.wantError {
				if err == nil {
					t.Error("expected error but got none")
				} else if err.Error() != tt.errorMsg {
					t.Errorf("expected error %q, got %q", tt.errorMsg, err.Error())
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
			}
		})
	}
}

func TestLoadFromFileInvalidJSON(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "test_invalid_config_*.json")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer func() { _ = os.Remove(tmpFile.Name()) }()

	if _, err := tmpFile.WriteString("{invalid json}"); err != nil {
		t.Fatalf("Failed to write to temp file: %v", err)
	}
	_ = tmpFile.Close()

	cfg := &Config{}
	err = LoadFromFile(tmpFile.Name(), cfg)
	if err == nil {
		t.Error("expected error for invalid JSON")
	}
}

func TestLoadFromFileNotFound(t *testing.T) {
	cfg := &Config{}
	err := LoadFromFile("/nonexistent/path/config.json", cfg)
	if err == nil {
		t.Error("expected error for missing file")
	}
}
