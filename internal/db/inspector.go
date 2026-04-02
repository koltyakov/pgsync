// Package db provides database inspection capabilities for PostgreSQL.
// It supports reading table metadata, columns, primary keys, and foreign key dependencies.
package db

import (
	"context"
	"database/sql"
	"fmt"
	"sync"

	"github.com/koltyakov/pgsync/internal/table"
	"github.com/lib/pq"
)

// Inspector provides database inspection capabilities.
// It is safe for concurrent use.
type Inspector struct {
	sourceDB *sql.DB
	targetDB *sql.DB
	schema   string
}

// NewInspector creates a new database inspector.
// Both sourceDB and targetDB may be nil if only certain operations are needed,
// but operations requiring them will return errors.
// Schema should be a valid PostgreSQL schema name (default: "public").
func NewInspector(sourceDB, targetDB *sql.DB, schema string) *Inspector {
	return &Inspector{
		sourceDB: sourceDB,
		targetDB: targetDB,
		schema:   schema,
	}
}

// GetTables returns all BASE TABLE names in the specified schema, sorted alphabetically.
// Excludes views, materialized views, and foreign tables.
func (i *Inspector) GetTables(ctx context.Context) ([]string, error) {
	if i == nil {
		return nil, fmt.Errorf("inspector is nil")
	}
	if i.sourceDB == nil {
		return nil, fmt.Errorf("source database connection is nil")
	}

	query := `
		SELECT table_name 
		FROM information_schema.tables 
		WHERE table_schema = $1 AND table_type = 'BASE TABLE'
		ORDER BY table_name`

	rows, err := i.sourceDB.QueryContext(ctx, query, i.schema)
	if err != nil {
		return nil, fmt.Errorf("failed to query tables: %w", err)
	}
	defer func() {
		_ = rows.Close()
	}()

	var tables []string
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return nil, fmt.Errorf("failed to scan table name: %w", err)
		}
		tables = append(tables, tableName)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating tables: %w", err)
	}

	return tables, nil
}

// GetTableInfo returns detailed information about a table.
// Runs queries concurrently for better performance.
// Returns error if tableName is empty or table doesn't exist.
func (i *Inspector) GetTableInfo(ctx context.Context, tableName string) (*table.Info, error) {
	if i == nil {
		return nil, fmt.Errorf("inspector is nil")
	}
	if i.sourceDB == nil {
		return nil, fmt.Errorf("source database connection is nil")
	}
	if tableName == "" {
		return nil, fmt.Errorf("tableName is empty")
	}

	info := &table.Info{
		Name:   tableName,
		Schema: i.schema,
	}

	// Run queries concurrently for better performance
	var wg sync.WaitGroup
	var columnsErr, pkErr, countErr error
	var columns, primaryKey []string
	var rowCount int64

	wg.Add(3)

	go func() {
		defer wg.Done()
		columns, columnsErr = i.getColumns(ctx, tableName)
	}()

	go func() {
		defer wg.Done()
		primaryKey, pkErr = i.getPrimaryKey(ctx, tableName)
	}()

	go func() {
		defer wg.Done()
		rowCount, countErr = i.getRowCount(ctx, tableName)
	}()

	wg.Wait()

	// Check context first - if canceled, return early without detailed errors
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	// Return first error encountered
	if columnsErr != nil {
		return nil, fmt.Errorf("failed to get columns: %w", columnsErr)
	}
	if pkErr != nil {
		return nil, fmt.Errorf("failed to get primary key: %w", pkErr)
	}
	if countErr != nil {
		return nil, fmt.Errorf("failed to get row count: %w", countErr)
	}

	info.Columns = columns
	info.PrimaryKey = primaryKey
	info.RowCount = rowCount

	return info, nil
}

// getColumns returns all columns for a table
func (i *Inspector) getColumns(ctx context.Context, tableName string) ([]string, error) {
	query := `
		SELECT column_name 
		FROM information_schema.columns 
		WHERE table_schema = $1 AND table_name = $2 
		ORDER BY ordinal_position`

	rows, err := i.sourceDB.QueryContext(ctx, query, i.schema, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to query columns: %w", err)
	}
	defer func() {
		_ = rows.Close()
	}()

	var columns []string
	for rows.Next() {
		var columnName string
		if err := rows.Scan(&columnName); err != nil {
			return nil, fmt.Errorf("failed to scan column name: %w", err)
		}
		columns = append(columns, columnName)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating columns: %w", err)
	}

	return columns, nil
}

// getPrimaryKey returns primary key columns for a table
func (i *Inspector) getPrimaryKey(ctx context.Context, tableName string) ([]string, error) {
	query := `
		SELECT a.attname
		FROM pg_index i
		JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
		WHERE i.indrelid = $1::regclass AND i.indisprimary
		ORDER BY array_position(i.indkey, a.attnum)`

	// Use pq.QuoteIdentifier to safely quote identifiers and prevent SQL injection
	fullTableName := pq.QuoteIdentifier(i.schema) + "." + pq.QuoteIdentifier(tableName)
	rows, err := i.sourceDB.QueryContext(ctx, query, fullTableName)
	if err != nil {
		return nil, fmt.Errorf("failed to query primary key for %s: %w", tableName, err)
	}
	defer func() {
		_ = rows.Close()
	}()

	var primaryKey []string
	for rows.Next() {
		var columnName string
		if err := rows.Scan(&columnName); err != nil {
			return nil, fmt.Errorf("failed to scan primary key column: %w", err)
		}
		primaryKey = append(primaryKey, columnName)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating primary key columns: %w", err)
	}

	return primaryKey, nil
}

// getRowCount returns actual row count for a table
// Uses COUNT(*) to get accurate results rather than estimates from pg_stat_user_tables
// which can be stale and not reflect recent inserts/deletes
func (i *Inspector) getRowCount(ctx context.Context, tableName string) (int64, error) {
	// Use pq.QuoteIdentifier to safely quote identifiers and prevent SQL injection
	countQuery := "SELECT COUNT(*) FROM " + pq.QuoteIdentifier(i.schema) + "." + pq.QuoteIdentifier(tableName)
	var count int64
	err := i.sourceDB.QueryRowContext(ctx, countQuery).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to count rows in %s: %w", tableName, err)
	}
	return count, nil
}

// TableDependency represents a foreign key dependency between tables
type TableDependency struct {
	Table      string // The table that has the FK
	DependsOn  string // The table that is referenced
	Constraint string // The FK constraint name
}

// GetTableDependencies returns all foreign key dependencies for tables in the schema
// This is used to determine the correct order for syncing tables
func (i *Inspector) GetTableDependencies(ctx context.Context) ([]TableDependency, error) {
	if i.sourceDB == nil {
		return nil, fmt.Errorf("source database connection is nil")
	}

	query := `
		SELECT 
			tc.table_name AS table_name,
			ccu.table_name AS referenced_table,
			tc.constraint_name
		FROM information_schema.table_constraints tc
		JOIN information_schema.constraint_column_usage ccu 
			ON tc.constraint_name = ccu.constraint_name 
			AND tc.table_schema = ccu.table_schema
		WHERE tc.constraint_type = 'FOREIGN KEY'
			AND tc.table_schema = $1
			AND ccu.table_schema = $1
		ORDER BY tc.table_name, ccu.table_name`

	rows, err := i.sourceDB.QueryContext(ctx, query, i.schema)
	if err != nil {
		return nil, fmt.Errorf("failed to query foreign key dependencies: %w", err)
	}
	defer func() {
		_ = rows.Close()
	}()

	var deps []TableDependency
	seen := make(map[string]struct{}) // Deduplicate (a table may have multiple FKs to same table)
	for rows.Next() {
		var dep TableDependency
		if err := rows.Scan(&dep.Table, &dep.DependsOn, &dep.Constraint); err != nil {
			return nil, fmt.Errorf("failed to scan dependency: %w", err)
		}
		// Skip self-references
		if dep.Table == dep.DependsOn {
			continue
		}
		key := dep.Table + "->" + dep.DependsOn
		if _, exists := seen[key]; !exists {
			seen[key] = struct{}{}
			deps = append(deps, dep)
		}
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating dependencies: %w", err)
	}

	return deps, nil
}

// TableExistsInTarget checks if a table exists in the target database
func (i *Inspector) TableExistsInTarget(ctx context.Context, tableName string) (bool, error) {
	if i.targetDB == nil {
		return false, nil
	}
	query := `
		SELECT EXISTS(
			SELECT 1 FROM information_schema.tables 
			WHERE table_schema = $1 AND table_name = $2 AND table_type = 'BASE TABLE'
		)`
	var exists bool
	err := i.targetDB.QueryRowContext(ctx, query, i.schema, tableName).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("failed to check table existence in target: %w", err)
	}
	return exists, nil
}

// GetTargetColumns returns columns that exist in the target table
func (i *Inspector) GetTargetColumns(ctx context.Context, tableName string) ([]string, error) {
	if i.targetDB == nil {
		return nil, nil
	}
	query := `
		SELECT column_name 
		FROM information_schema.columns 
		WHERE table_schema = $1 AND table_name = $2 
		ORDER BY ordinal_position`

	rows, err := i.targetDB.QueryContext(ctx, query, i.schema, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to query target columns: %w", err)
	}
	defer func() {
		_ = rows.Close()
	}()

	var columns []string
	for rows.Next() {
		var col string
		if err := rows.Scan(&col); err != nil {
			return nil, fmt.Errorf("failed to scan target column: %w", err)
		}
		columns = append(columns, col)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating target columns: %w", err)
	}

	return columns, nil
}
