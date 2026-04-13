package syncer

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/koltyakov/pgsync/internal/table"
	"github.com/lib/pq"
)

// BulkBatchSize defines the number of rows per bulk INSERT statement.
// 500 rows provides optimal balance between query size and PostgreSQL's
// multi-value INSERT performance. Larger batches reduce round-trips.
const BulkBatchSize = 500

// CopyThreshold defines minimum rows to use COPY protocol instead of INSERT.
// COPY is faster for large batches but has overhead for small ones.
const CopyThreshold = 1000

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

	// For large batches, use COPY protocol with temp table for better performance
	if len(rows) >= CopyThreshold {
		return s.upsertDataWithCopy(ctx, tableInfo, rows)
	}

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

// upsertDataWithCopy uses PostgreSQL COPY protocol for high-performance bulk upserts.
// It copies data to a temp table, then merges into the target table.
func (s *Syncer) upsertDataWithCopy(ctx context.Context, tableInfo *table.Info, rows [][]any) error {
	tx, err := s.targetDB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	// Defer all FK constraints
	if _, err := tx.ExecContext(ctx, "SET CONSTRAINTS ALL DEFERRED"); err != nil {
		s.logger.Debug("Could not defer constraints", "table", tableInfo.Name)
	}

	// Create temp table with same structure
	tempTable := fmt.Sprintf("_pgsync_temp_%s", tableInfo.Name)
	createTempSQL := fmt.Sprintf(
		"CREATE TEMP TABLE %s (LIKE %s INCLUDING ALL) ON COMMIT DROP",
		pq.QuoteIdentifier(tempTable),
		s.quotedTableName(tableInfo.Name),
	)
	if _, err := tx.ExecContext(ctx, createTempSQL); err != nil {
		return fmt.Errorf("failed to create temp table: %w", err)
	}

	// Use COPY to load data into temp table
	stmt, err := tx.PrepareContext(ctx, pq.CopyIn(tempTable, tableInfo.Columns...))
	if err != nil {
		return fmt.Errorf("failed to prepare COPY: %w", err)
	}

	for _, row := range rows {
		convertedRow := make([]any, len(row))
		for i, val := range row {
			colType := ""
			if i < len(tableInfo.DataTypes) {
				colType = tableInfo.DataTypes[i]
			}
			convertedRow[i] = convertForCopy(val, colType)
		}
		if _, err := stmt.ExecContext(ctx, convertedRow...); err != nil {
			_ = stmt.Close()
			return fmt.Errorf("failed to COPY row: %w", err)
		}
	}

	if _, err := stmt.ExecContext(ctx); err != nil {
		_ = stmt.Close()
		return fmt.Errorf("failed to flush COPY: %w", err)
	}
	_ = stmt.Close()

	// Merge from temp table to target using INSERT ... ON CONFLICT
	columns := s.quotedColumnsList(tableInfo.Columns)
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

	//nolint:gosec // G201 - table/column names are safely quoted
	mergeSQL := fmt.Sprintf(`
		INSERT INTO %s (%s)
		SELECT %s FROM %s
		ON CONFLICT (%s) DO UPDATE SET %s`,
		s.quotedTableName(tableInfo.Name), columns,
		columns, pq.QuoteIdentifier(tempTable),
		conflictCols, updateClause,
	)

	if _, err := tx.ExecContext(ctx, mergeSQL); err != nil {
		return fmt.Errorf("failed to merge from temp table: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	s.addUpserts(tableInfo.Name, len(rows))
	s.logger.Debug("COPY upsert completed", "table", tableInfo.Name, "rows", len(rows))

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

// fetchRowsByPK fetches full rows from the given database by their primary key values
func (s *Syncer) fetchRowsByPK(ctx context.Context, db *sql.DB, tableInfo *table.Info, pkValues [][]any) ([][]any, error) {
	if len(pkValues) == 0 {
		return nil, nil
	}

	columns := s.quotedColumnsList(tableInfo.Columns)

	var conditions []string
	var args []any
	argIdx := 1

	for _, pk := range pkValues {
		var pkConds []string
		for i, col := range tableInfo.PrimaryKey {
			pkConds = append(pkConds, fmt.Sprintf("%s = $%d", s.quotedColumnName(col), argIdx))
			args = append(args, pk[i])
			argIdx++
		}
		conditions = append(conditions, "("+strings.Join(pkConds, " AND ")+")")
	}

	//nolint:gosec // G201 - table/column names are safely quoted and values remain parameterized
	query := fmt.Sprintf("SELECT %s FROM %s WHERE %s",
		columns,
		s.quotedTableName(tableInfo.Name),
		strings.Join(conditions, " OR "),
	)

	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query rows by PK: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var result [][]any
	for rows.Next() {
		values := make([]any, len(tableInfo.Columns))
		scanArgs := make([]any, len(tableInfo.Columns))
		for i := range values {
			scanArgs[i] = &values[i]
		}
		if err := rows.Scan(scanArgs...); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}
		result = append(result, values)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %w", err)
	}

	return result, nil
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
// Uses pq.QuoteIdentifier to prevent SQL injection
func (s *Syncer) quotedTableName(tableName string) string {
	return pq.QuoteIdentifier(s.cfg.Schema) + "." + pq.QuoteIdentifier(tableName)
}

// quotedColumnName returns a properly quoted column name for PostgreSQL
// Uses pq.QuoteIdentifier to prevent SQL injection
func (s *Syncer) quotedColumnName(columnName string) string {
	return pq.QuoteIdentifier(columnName)
}

// quotedColumnsList returns a comma-separated list of quoted column names
func (s *Syncer) quotedColumnsList(columns []string) string {
	quoted := make([]string, len(columns))
	for i, col := range columns {
		quoted[i] = s.quotedColumnName(col)
	}
	return strings.Join(quoted, ", ")
}

// convertForCopy converts values to formats compatible with PostgreSQL COPY protocol.
// Handles UUID byte arrays, numeric types, and other special cases.
func convertForCopy(val any, colType string) any {
	if val == nil {
		return nil
	}

	if b, ok := val.([]byte); ok {
		if colType == "uuid" {
			switch {
			case len(b) == 16 && !isPrintableASCII(b):
				return formatUUID(b)
			default:
				return string(b)
			}
		}

		str := string(b)

		if len(str) == 36 && str[8] == '-' && str[13] == '-' && str[18] == '-' && str[23] == '-' {
			return str
		}

		if len(b) == 16 && !isPrintableASCII(b) {
			return formatUUID(b)
		}

		if len(b) > 0 && isPrintableASCII(b) {
			return str
		}

		return val
	}

	return val
}

// isPrintableASCII checks if all bytes are printable ASCII characters
func isPrintableASCII(b []byte) bool {
	for _, c := range b {
		if c < 32 || c > 126 {
			return false
		}
	}
	return true
}

// formatUUID converts a 16-byte UUID to standard string format (xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx)
func formatUUID(b []byte) string {
	if len(b) != 16 {
		return string(b)
	}
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}
