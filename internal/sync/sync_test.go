package sync

import (
	"testing"

	"github.com/koltyakov/pgsync/internal/config"
)

func TestMatchesPattern(t *testing.T) {
	syncer := &Syncer{
		cfg: &config.Config{},
	}

	tests := []struct {
		name      string
		tableName string
		patterns  []string
		expected  bool
	}{
		{
			name:      "exact match",
			tableName: "users",
			patterns:  []string{"users", "orders"},
			expected:  true,
		},
		{
			name:      "no match",
			tableName: "products",
			patterns:  []string{"users", "orders"},
			expected:  false,
		},
		{
			name:      "wildcard star match",
			tableName: "user_profiles",
			patterns:  []string{"user_*"},
			expected:  true,
		},
		{
			name:      "wildcard star no match",
			tableName: "products",
			patterns:  []string{"user_*"},
			expected:  false,
		},
		{
			name:      "wildcard question mark match",
			tableName: "temp_001",
			patterns:  []string{"temp_???"},
			expected:  true,
		},
		{
			name:      "wildcard question mark no match",
			tableName: "temp_1234",
			patterns:  []string{"temp_???"},
			expected:  false,
		},
		{
			name:      "wildcard range match",
			tableName: "audit_2023",
			patterns:  []string{"audit_[0-9]*"},
			expected:  true,
		},
		{
			name:      "wildcard range no match",
			tableName: "audit_test",
			patterns:  []string{"audit_[0-9]*"},
			expected:  false,
		},
		{
			name:      "multiple patterns with match",
			tableName: "error_log",
			patterns:  []string{"temp_*", "*_log", "audit_*"},
			expected:  true,
		},
		{
			name:      "multiple patterns no match",
			tableName: "products",
			patterns:  []string{"temp_*", "*_log", "audit_*"},
			expected:  false,
		},
		{
			name:      "empty patterns",
			tableName: "users",
			patterns:  []string{},
			expected:  false,
		},
		{
			name:      "whitespace in patterns",
			tableName: "users",
			patterns:  []string{" users ", "orders"},
			expected:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := syncer.matchesPattern(tt.tableName, tt.patterns)
			if result != tt.expected {
				t.Errorf("matchesPattern(%q, %v) = %v, expected %v", 
					tt.tableName, tt.patterns, result, tt.expected)
			}
		})
	}
}
