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
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return nil, fmt.Errorf("failed to scan table name: %w", err)
		}
		tables = append(tables, tableName)
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
		return nil, err
	}
	defer rows.Close()

	var columns []string
	for rows.Next() {
		var columnName string
		if err := rows.Scan(&columnName); err != nil {
			return nil, err
		}
		columns = append(columns, columnName)
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
		return nil, err
	}
	defer rows.Close()

	var primaryKey []string
	for rows.Next() {
		var columnName string
		if err := rows.Scan(&columnName); err != nil {
			return nil, err
		}
		primaryKey = append(primaryKey, columnName)
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
