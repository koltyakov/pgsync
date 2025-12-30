package sync

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/koltyakov/pgsync/internal/table"
)

// BulkBatchSize defines the number of rows per bulk INSERT statement
const BulkBatchSize = 100

// upsertData performs upsert operation on target table using bulk inserts
func (s *Syncer) upsertData(ctx context.Context, tableInfo *table.Info, rows [][]any) error {
	if len(rows) == 0 {
		return nil
	}

	if s.cfg.DryRun {
		s.logger.Info("[DRY-RUN] Would upsert rows", "table", tableInfo.Name, "count", len(rows))
		s.addUpserts(tableInfo.Name, len(rows))
		return nil
	}

	// Use transaction for batch atomicity
	tx, err := s.targetDB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	// Defer all FK constraints to end of transaction to handle self-referential tables
	// (e.g., users.manager_id -> users.id, product_categories.parent_id -> product_categories.id)
	if _, err := tx.ExecContext(ctx, "SET CONSTRAINTS ALL DEFERRED"); err != nil {
		// If deferred constraints aren't supported or not set as deferrable, continue without
		s.logger.Debug("Could not defer constraints (may not be deferrable)", "table", tableInfo.Name)
	}

	// Process rows in bulk batches
	for i := 0; i < len(rows); i += BulkBatchSize {
		end := i + BulkBatchSize
		if end > len(rows) {
			end = len(rows)
		}
		batch := rows[i:end]

		if err := s.bulkUpsertBatch(ctx, tx, tableInfo, batch); err != nil {
			return err
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	s.addUpserts(tableInfo.Name, len(rows))

	return nil
}

// bulkUpsertBatch performs a multi-value INSERT for a batch of rows
func (s *Syncer) bulkUpsertBatch(ctx context.Context, tx *sql.Tx, tableInfo *table.Info, rows [][]any) error {
	if len(rows) == 0 {
		return nil
	}

	query, args := s.buildBulkUpsertQuery(tableInfo, rows)
	_, err := tx.ExecContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("failed to bulk upsert: %w", err)
	}

	return nil
}

// buildBulkUpsertQuery builds a multi-value INSERT ... ON CONFLICT query
func (s *Syncer) buildBulkUpsertQuery(tableInfo *table.Info, rows [][]any) (string, []any) {
	columns := s.quotedColumnsList(tableInfo.Columns)
	colCount := len(tableInfo.Columns)

	// Build value placeholders for all rows
	var valueSets []string
	var args []any
	paramNum := 1

	for _, row := range rows {
		placeholders := make([]string, colCount)
		for i := 0; i < colCount; i++ {
			placeholders[i] = fmt.Sprintf("$%d", paramNum)
			paramNum++
		}
		valueSets = append(valueSets, "("+strings.Join(placeholders, ", ")+")")
		args = append(args, row...)
	}

	// Build ON CONFLICT clause
	conflictCols := s.quotedColumnsList(tableInfo.PrimaryKey)

	// Build UPDATE SET clause (exclude primary key columns)
	var updateParts []string
	for _, col := range tableInfo.Columns {
		isPK := false
		for _, pk := range tableInfo.PrimaryKey {
			if col == pk {
				isPK = true
				break
			}
		}
		if !isPK {
			quotedCol := s.quotedColumnName(col)
			updateParts = append(updateParts, fmt.Sprintf("%s = EXCLUDED.%s", quotedCol, quotedCol))
		}
	}
	updateClause := strings.Join(updateParts, ", ")

	query := fmt.Sprintf(`
		INSERT INTO %s (%s) 
		VALUES %s 
		ON CONFLICT (%s) 
		DO UPDATE SET %s`,
		s.quotedTableName(tableInfo.Name), columns, strings.Join(valueSets, ", "), conflictCols, updateClause)

	return query, args
}

// buildUpsertQuery builds an upsert query for PostgreSQL (single row)
func (s *Syncer) buildUpsertQuery(tableInfo *table.Info) string {
	columns := s.quotedColumnsList(tableInfo.Columns)
	placeholders := make([]string, len(tableInfo.Columns))
	for i := range placeholders {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
	}
	placeholderStr := strings.Join(placeholders, ", ")

	// Build ON CONFLICT clause
	conflictCols := s.quotedColumnsList(tableInfo.PrimaryKey)

	// Build UPDATE SET clause (exclude primary key columns)
	var updateParts []string
	for _, col := range tableInfo.Columns {
		isPK := false
		for _, pk := range tableInfo.PrimaryKey {
			if col == pk {
				isPK = true
				break
			}
		}
		if !isPK {
			quotedCol := s.quotedColumnName(col)
			updateParts = append(updateParts, fmt.Sprintf("%s = EXCLUDED.%s", quotedCol, quotedCol))
		}
	}
	updateClause := strings.Join(updateParts, ", ")

	query := fmt.Sprintf(`
		INSERT INTO %s (%s) 
		VALUES (%s) 
		ON CONFLICT (%s) 
		DO UPDATE SET %s`,
		s.quotedTableName(tableInfo.Name), columns, placeholderStr, conflictCols, updateClause)

	return query
}

// buildPKWhereClause builds a WHERE clause for primary key matching
func (s *Syncer) buildPKWhereClause(pkCols []string) string {
	parts := make([]string, len(pkCols))
	for i, col := range pkCols {
		parts[i] = fmt.Sprintf("%s = $%d", s.quotedColumnName(col), i+1)
	}
	return strings.Join(parts, " AND ")
}

// createPKString creates a string representation of primary key values
func (s *Syncer) createPKString(values []any) string {
	parts := make([]string, len(values))
	for i, v := range values {
		parts[i] = fmt.Sprintf("%v", v)
	}
	return strings.Join(parts, "|")
}

// quotedTableName returns a properly quoted table name for PostgreSQL
func (s *Syncer) quotedTableName(tableName string) string {
	return fmt.Sprintf("%s.\"%s\"", s.cfg.Schema, tableName)
}

// quotedColumnName returns a properly quoted column name for PostgreSQL
func (s *Syncer) quotedColumnName(columnName string) string {
	return fmt.Sprintf("\"%s\"", columnName)
}

// quotedColumnsList returns a comma-separated list of quoted column names
func (s *Syncer) quotedColumnsList(columns []string) string {
	quoted := make([]string, len(columns))
	for i, col := range columns {
		quoted[i] = s.quotedColumnName(col)
	}
	return strings.Join(quoted, ", ")
}
