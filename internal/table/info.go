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
