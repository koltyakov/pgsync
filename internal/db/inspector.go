package db

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/koltyakov/pgsync/internal/table"
)

// Inspector provides database inspection capabilities
type Inspector struct {
	sourceDB *sql.DB
	targetDB *sql.DB
	schema   string
}

// NewInspector creates a new database inspector
func NewInspector(sourceDB, targetDB *sql.DB, schema string) *Inspector {
	return &Inspector{
		sourceDB: sourceDB,
		targetDB: targetDB,
		schema:   schema,
	}
}

// GetTables returns all tables in the specified schema
func (i *Inspector) GetTables(ctx context.Context) ([]string, error) {
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
		_ = rows.Close() // Ignore error in deferred close
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

// GetTableInfo returns detailed information about a table
func (i *Inspector) GetTableInfo(ctx context.Context, tableName string) (*table.Info, error) {
	info := &table.Info{
		Name:   tableName,
		Schema: i.schema,
	}

	// Get columns
	columns, err := i.getColumns(ctx, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to get columns: %w", err)
	}
	info.Columns = columns

	// Get primary key
	primaryKey, err := i.getPrimaryKey(ctx, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to get primary key: %w", err)
	}
	info.PrimaryKey = primaryKey

	// Get row count estimate
	rowCount, err := i.getRowCount(ctx, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to get row count: %w", err)
	}
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
		_ = rows.Close() // Ignore error in deferred close
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

	// Quote the table name to handle CamelCase names from .NET Entity Framework
	fullTableName := fmt.Sprintf("%s.\"%s\"", i.schema, tableName)
	rows, err := i.sourceDB.QueryContext(ctx, query, fullTableName)
	if err != nil {
		return nil, fmt.Errorf("failed to query primary key for %s: %w", tableName, err)
	}
	defer func() {
		_ = rows.Close() // Ignore error in deferred close
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

// getRowCount returns estimated row count for a table
func (i *Inspector) getRowCount(ctx context.Context, tableName string) (int64, error) {
	query := `
		SELECT COALESCE(n_tup_ins - n_tup_del, 0) as estimate
		FROM pg_stat_user_tables 
		WHERE schemaname = $1 AND relname = $2`

	var count sql.NullInt64
	err := i.sourceDB.QueryRowContext(ctx, query, i.schema, tableName).Scan(&count)
	if err != nil {
		// Fallback to actual count if stats are not available
		// Quote the table name to handle CamelCase names from .NET Entity Framework
		countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s.\"%s\"", i.schema, tableName)
		err = i.sourceDB.QueryRowContext(ctx, countQuery).Scan(&count)
		if err != nil {
			return 0, err
		}
	}

	if count.Valid {
		return count.Int64, nil
	}
	return 0, nil
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
	seen := make(map[string]bool) // Deduplicate (a table may have multiple FKs to same table)
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
		if !seen[key] {
			seen[key] = true
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
		return false, fmt.Errorf("failed to check table existence: %w", err)
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
	defer func() { _ = rows.Close() }()

	var columns []string
	for rows.Next() {
		var col string
		if err := rows.Scan(&col); err != nil {
			return nil, fmt.Errorf("failed to scan column: %w", err)
		}
		columns = append(columns, col)
	}
	return columns, rows.Err()
}
