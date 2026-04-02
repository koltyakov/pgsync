package sync //nolint:revive // intentionally shadows stdlib sync

import (
	"testing"
	"time"

	"github.com/koltyakov/pgsync/internal/config"
	"github.com/koltyakov/pgsync/internal/db"
	"github.com/koltyakov/pgsync/internal/table"
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

func TestBuildUpsertQuery(t *testing.T) {
	syncer := &Syncer{
		cfg: &config.Config{
			Schema: "public",
		},
	}

	tests := []struct {
		name     string
		info     *table.Info
		contains []string
	}{
		{
			name: "single column PK",
			info: &table.Info{
				Name:       "users",
				Schema:     "public",
				Columns:    []string{"id", "name", "email"},
				PrimaryKey: []string{"id"},
			},
			contains: []string{
				"INSERT INTO \"public\".\"users\"",
				"\"id\", \"name\", \"email\"",
				"$1, $2, $3",
				"ON CONFLICT (\"id\")",
				"\"name\" = EXCLUDED.\"name\"",
				"\"email\" = EXCLUDED.\"email\"",
			},
		},
		{
			name: "composite PK",
			info: &table.Info{
				Name:       "order_items",
				Schema:     "public",
				Columns:    []string{"order_id", "item_id", "quantity", "price"},
				PrimaryKey: []string{"order_id", "item_id"},
			},
			contains: []string{
				"INSERT INTO \"public\".\"order_items\"",
				"ON CONFLICT (\"order_id\", \"item_id\")",
				"\"quantity\" = EXCLUDED.\"quantity\"",
				"\"price\" = EXCLUDED.\"price\"",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			query := syncer.buildUpsertQuery(tt.info)
			for _, substr := range tt.contains {
				if !contains(query, substr) {
					t.Errorf("query missing expected substring %q\nQuery: %s", substr, query)
				}
			}
		})
	}
}

func TestBuildPKWhereClause(t *testing.T) {
	syncer := &Syncer{
		cfg: &config.Config{},
	}

	tests := []struct {
		name     string
		pkCols   []string
		expected string
	}{
		{
			name:     "single column",
			pkCols:   []string{"id"},
			expected: "\"id\" = $1",
		},
		{
			name:     "composite key",
			pkCols:   []string{"order_id", "item_id"},
			expected: "\"order_id\" = $1 AND \"item_id\" = $2",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := syncer.buildPKWhereClause(tt.pkCols)
			if result != tt.expected {
				t.Errorf("expected %q, got %q", tt.expected, result)
			}
		})
	}
}

func TestCreatePKString(t *testing.T) {
	syncer := &Syncer{}

	tests := []struct {
		name     string
		values   []any
		expected string
	}{
		{
			name:     "single value",
			values:   []any{123},
			expected: "123",
		},
		{
			name:     "multiple values",
			values:   []any{1, "abc", 3.14},
			expected: "1|abc|3.14",
		},
		{
			name:     "nil values",
			values:   []any{nil, "test"},
			expected: "<nil>|test",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := syncer.createPKString(tt.values)
			if result != tt.expected {
				t.Errorf("expected %q, got %q", tt.expected, result)
			}
		})
	}
}

func TestQuotedTableName(t *testing.T) {
	syncer := &Syncer{
		cfg: &config.Config{Schema: "myschema"},
	}

	result := syncer.quotedTableName("MyTable")
	// pq.QuoteIdentifier quotes both schema and table name
	expected := "\"myschema\".\"MyTable\""
	if result != expected {
		t.Errorf("expected %q, got %q", expected, result)
	}
}

func TestQuotedColumnName(t *testing.T) {
	syncer := &Syncer{}

	result := syncer.quotedColumnName("ColumnName")
	expected := "\"ColumnName\""
	if result != expected {
		t.Errorf("expected %q, got %q", expected, result)
	}
}

func TestQuotedColumnsList(t *testing.T) {
	syncer := &Syncer{}

	result := syncer.quotedColumnsList([]string{"col1", "col2", "col3"})
	expected := "\"col1\", \"col2\", \"col3\""
	if result != expected {
		t.Errorf("expected %q, got %q", expected, result)
	}
}

func TestRowsEqual(t *testing.T) {
	tests := []struct {
		name     string
		a        []any
		b        []any
		expected bool
	}{
		{
			name:     "equal rows",
			a:        []any{1, "test", 3.14},
			b:        []any{1, "test", 3.14},
			expected: true,
		},
		{
			name:     "different values",
			a:        []any{1, "test", 3.14},
			b:        []any{2, "test", 3.14},
			expected: false,
		},
		{
			name:     "different lengths",
			a:        []any{1, "test"},
			b:        []any{1, "test", 3.14},
			expected: false,
		},
		{
			name:     "both nil",
			a:        nil,
			b:        nil,
			expected: true,
		},
		{
			name:     "empty slices",
			a:        []any{},
			b:        []any{},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := rowsEqual(tt.a, tt.b)
			if result != tt.expected {
				t.Errorf("expected %v, got %v", tt.expected, result)
			}
		})
	}
}

func TestFormatHumanDuration(t *testing.T) {
	tests := []struct {
		name     string
		duration string
		expected string
	}{
		{"zero", "0s", "0ms"},
		{"milliseconds only", "500ms", "500ms"},
		{"seconds only", "5s", "5s"},
		{"seconds and ms", "5s500ms", "5s500ms"},
		{"minutes and seconds", "2m30s", "2m30s"},
		{"hours minutes seconds", "1h30m45s", "1h30m45s"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d, _ := time.ParseDuration(tt.duration)
			result := formatHumanDuration(d)
			if result != tt.expected {
				t.Errorf("formatHumanDuration(%s) = %q, expected %q", tt.duration, result, tt.expected)
			}
		})
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsAt(s, substr, 0))
}

func containsAt(s, substr string, start int) bool {
	for i := start; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

func TestTopologicalSort(t *testing.T) {
	tests := []struct {
		name     string
		tables   []string
		deps     []db.TableDependency
		validate func([]string) bool
	}{
		{
			name:   "no dependencies",
			tables: []string{"users", "products", "orders"},
			deps:   nil,
			validate: func(result []string) bool {
				return len(result) == 3
			},
		},
		{
			name:   "simple chain: orders -> users",
			tables: []string{"orders", "users"},
			deps: []db.TableDependency{
				{Table: "orders", DependsOn: "users", Constraint: "orders_user_id_fkey"},
			},
			validate: func(result []string) bool {
				// users must come before orders
				usersIdx, ordersIdx := -1, -1
				for i, t := range result {
					if t == "users" {
						usersIdx = i
					}
					if t == "orders" {
						ordersIdx = i
					}
				}
				return usersIdx < ordersIdx && len(result) == 2
			},
		},
		{
			name:   "complex CRM dependencies",
			tables: []string{"contacts", "organizations", "activities", "deals"},
			deps: []db.TableDependency{
				{Table: "contacts", DependsOn: "organizations", Constraint: "contacts_org_fkey"},
				{Table: "activities", DependsOn: "contacts", Constraint: "activities_contact_fkey"},
				{Table: "deals", DependsOn: "contacts", Constraint: "deals_contact_fkey"},
			},
			validate: func(result []string) bool {
				// organizations must come before contacts
				// contacts must come before activities and deals
				indexOf := func(name string) int {
					for i, t := range result {
						if t == name {
							return i
						}
					}
					return -1
				}
				orgsIdx := indexOf("organizations")
				contactsIdx := indexOf("contacts")
				activitiesIdx := indexOf("activities")
				dealsIdx := indexOf("deals")

				return orgsIdx < contactsIdx &&
					contactsIdx < activitiesIdx &&
					contactsIdx < dealsIdx &&
					len(result) == 4
			},
		},
		{
			name:   "multiple dependencies on same parent",
			tables: []string{"parent", "child1", "child2", "grandchild"},
			deps: []db.TableDependency{
				{Table: "child1", DependsOn: "parent", Constraint: "child1_parent_fkey"},
				{Table: "child2", DependsOn: "parent", Constraint: "child2_parent_fkey"},
				{Table: "grandchild", DependsOn: "child1", Constraint: "grandchild_child1_fkey"},
			},
			validate: func(result []string) bool {
				indexOf := func(name string) int {
					for i, t := range result {
						if t == name {
							return i
						}
					}
					return -1
				}
				parentIdx := indexOf("parent")
				child1Idx := indexOf("child1")
				child2Idx := indexOf("child2")
				grandchildIdx := indexOf("grandchild")

				return parentIdx < child1Idx &&
					parentIdx < child2Idx &&
					child1Idx < grandchildIdx &&
					len(result) == 4
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tableSet := make(map[string]struct{})
			for _, t := range tt.tables {
				tableSet[t] = struct{}{}
			}
			result := topologicalSort(tt.tables, tt.deps, tableSet)
			if !tt.validate(result) {
				t.Errorf("topologicalSort failed, got order: %v", result)
			}
		})
	}
}

func TestGroupByDependencyLevel(t *testing.T) {
	tests := []struct {
		name           string
		tables         []*table.Info
		deps           []db.TableDependency
		expectedLevels int
	}{
		{
			name: "no dependencies - all in one level",
			tables: []*table.Info{
				{Name: "users"},
				{Name: "products"},
			},
			deps:           nil,
			expectedLevels: 1,
		},
		{
			name: "chain creates multiple levels",
			tables: []*table.Info{
				{Name: "organizations"},
				{Name: "contacts"},
				{Name: "activities"},
			},
			deps: []db.TableDependency{
				{Table: "contacts", DependsOn: "organizations"},
				{Table: "activities", DependsOn: "contacts"},
			},
			expectedLevels: 3,
		},
		{
			name: "siblings at same level",
			tables: []*table.Info{
				{Name: "parent"},
				{Name: "child1"},
				{Name: "child2"},
			},
			deps: []db.TableDependency{
				{Table: "child1", DependsOn: "parent"},
				{Table: "child2", DependsOn: "parent"},
			},
			expectedLevels: 2, // parent at level 0, both children at level 1
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tableSet := make(map[string]struct{})
			for _, info := range tt.tables {
				tableSet[info.Name] = struct{}{}
			}
			levels := groupByDependencyLevel(tt.tables, tt.deps, tableSet)
			if len(levels) != tt.expectedLevels {
				t.Errorf("expected %d levels, got %d", tt.expectedLevels, len(levels))
			}
		})
	}
}
