package table

import "strings"

// Info holds metadata about a database table
type Info struct {
	Name       string   // Table name
	Schema     string   // Schema name
	Columns    []string // Column names in order
	PrimaryKey []string // Primary key column names
	RowCount   int64    // Estimated row count
}

// HasColumn checks if the table has a specific column
func (t *Info) HasColumn(columnName string) bool {
	for _, col := range t.Columns {
		if strings.EqualFold(col, columnName) {
			return true
		}
	}
	return false
}

// EstimatedWork calculates a rough estimate of work required for this table
// This is used for load balancing across workers
func (t *Info) EstimatedWork() int64 {
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

// FullName returns the fully qualified table name
func (t *Info) FullName() string {
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

	// Filter columns while preserving order and original case
	var filtered []string
	for _, col := range t.Columns {
		if includeSet[strings.ToLower(col)] {
			filtered = append(filtered, col)
		}
	}

	return &Info{
		Name:       t.Name,
		Schema:     t.Schema,
		Columns:    filtered,
		PrimaryKey: t.PrimaryKey,
		RowCount:   t.RowCount,
	}
}
