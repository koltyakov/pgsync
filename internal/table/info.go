// Package table provides data structures for representing database table metadata.
package table

import "strings"

// Info holds metadata about a database table.
// This struct is used throughout the sync process to track table structure and state.
type Info struct {
	Name       string   // Table name (unqualified)
	Schema     string   // Schema name (e.g., "public")
	Columns    []string // Column names in order as they appear in the database
	DataTypes  []string // PostgreSQL data type names (e.g., "uuid", "text", "integer")
	PrimaryKey []string // Primary key column names (may be empty for tables without PK)
	RowCount   int64    // Estimated row count (may be from pg_stat or exact COUNT)
}

// HasColumn checks if the table has a specific column.
// Comparison is case-insensitive to handle PostgreSQL's identifier normalization.
func (t *Info) HasColumn(columnName string) bool {
	if t == nil {
		return false
	}
	for _, col := range t.Columns {
		if strings.EqualFold(col, columnName) {
			return true
		}
	}
	return false
}

// EstimatedWork calculates a rough estimate of work required for this table.
// This is used for load balancing across workers when processing multiple tables.
// Higher values indicate more work/complexity.
func (t *Info) EstimatedWork() int64 {
	if t == nil {
		return 0
	}

	// Base work is row count
	work := t.RowCount

	// Add complexity factor based on column count
	work += int64(len(t.Columns)) * 10

	// Add complexity if no primary key (requires full table comparison)
	if len(t.PrimaryKey) == 0 {
		work *= 2
	}

	return work
}

// FullName returns the fully qualified table name in "schema.table" format.
// Returns empty string if Info is nil.
func (t *Info) FullName() string {
	if t == nil {
		return ""
	}
	return t.Schema + "." + t.Name
}

// FilterColumns returns a new Info with only the specified columns.
// Primary key columns are always included even if not in the filter.
// Uses case-insensitive matching because PostgreSQL normalizes unquoted
// identifiers to lowercase, so user input may differ in case from stored names.
//
// Note: When includeColumns is empty, returns the original Info pointer for efficiency.
// Callers should not modify the returned Info if they need to preserve the original.
func (t *Info) FilterColumns(includeColumns []string) *Info {
	if len(includeColumns) == 0 {
		return t // No filter, return original for efficiency
	}

	// Build a set of columns to include (always include PKs)
	// Use lowercase keys for case-insensitive matching
	includeSet := make(map[string]bool)
	for _, col := range includeColumns {
		includeSet[strings.ToLower(col)] = true
	}
	for _, pk := range t.PrimaryKey {
		includeSet[strings.ToLower(pk)] = true
	}

	var filtered []string
	var filteredTypes []string
	for i, col := range t.Columns {
		if includeSet[strings.ToLower(col)] {
			filtered = append(filtered, col)
			if i < len(t.DataTypes) {
				filteredTypes = append(filteredTypes, t.DataTypes[i])
			}
		}
	}

	return &Info{
		Name:       t.Name,
		Schema:     t.Schema,
		Columns:    filtered,
		DataTypes:  filteredTypes,
		PrimaryKey: t.PrimaryKey,
		RowCount:   t.RowCount,
	}
}
