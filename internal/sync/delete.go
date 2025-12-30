package sync

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/koltyakov/pgsync/internal/constants"
	"github.com/koltyakov/pgsync/internal/table"
)

// handleDeletedRows identifies and removes deleted rows from target
func (s *Syncer) handleDeletedRows(ctx context.Context, tableInfo *table.Info) error {
	if len(tableInfo.PrimaryKey) == 0 {
		s.logger.Debug("Table has no primary key, skipping deleted rows handling", "table", tableInfo.Name)
		return nil
	}

	pkCols := s.quotedColumnsList(tableInfo.PrimaryKey)

	// Get all primary keys from source
	sourceQuery := fmt.Sprintf("SELECT %s FROM %s", pkCols, s.quotedTableName(tableInfo.Name))
	sourceRows, err := s.sourceDB.QueryContext(ctx, sourceQuery)
	if err != nil {
		return fmt.Errorf("failed to query source primary keys: %w", err)
	}
	defer func() { _ = sourceRows.Close() }()

	sourcePKs := make(map[string]bool)
	for sourceRows.Next() {
		values := make([]any, len(tableInfo.PrimaryKey))
		scanArgs := make([]any, len(tableInfo.PrimaryKey))
		for i := range values {
			scanArgs[i] = &values[i]
		}

		if err := sourceRows.Scan(scanArgs...); err != nil {
			return fmt.Errorf("failed to scan source primary key: %w", err)
		}

		pkStr := s.createPKString(values)
		sourcePKs[pkStr] = true
	}

	if err := sourceRows.Err(); err != nil {
		return fmt.Errorf("error iterating source rows: %w", err)
	}

	// Get all primary keys from target and identify deletions
	targetQuery := fmt.Sprintf("SELECT %s FROM %s", pkCols, s.quotedTableName(tableInfo.Name))
	targetRows, err := s.targetDB.QueryContext(ctx, targetQuery)
	if err != nil {
		return fmt.Errorf("failed to query target primary keys: %w", err)
	}
	defer func() { _ = targetRows.Close() }()

	var toDelete [][]any
	for targetRows.Next() {
		values := make([]any, len(tableInfo.PrimaryKey))
		scanArgs := make([]any, len(tableInfo.PrimaryKey))
		for i := range values {
			scanArgs[i] = &values[i]
		}

		if err := targetRows.Scan(scanArgs...); err != nil {
			return fmt.Errorf("failed to scan target primary key: %w", err)
		}

		pkStr := s.createPKString(values)
		if !sourcePKs[pkStr] {
			toDelete = append(toDelete, values)
			// Chunked deletion using batch size to avoid large statements
			if s.cfg.BatchSize > 0 && len(toDelete) >= s.cfg.BatchSize {
				s.logger.Debug("Deleting rows chunk", "table", tableInfo.Name, "count", len(toDelete))
				if err := s.deleteRows(ctx, tableInfo, toDelete); err != nil {
					return fmt.Errorf("failed to delete rows chunk: %w", err)
				}
				s.addDeletes(tableInfo.Name, len(toDelete))
				toDelete = toDelete[:0]
			}
		}
	}

	if err := targetRows.Err(); err != nil {
		return fmt.Errorf("error iterating target rows: %w", err)
	}

	// Delete remaining rows that exist in target but not in source
	if len(toDelete) > 0 {
		s.logger.Debug("Deleting rows", "table", tableInfo.Name, "count", len(toDelete))

		if err := s.deleteRows(ctx, tableInfo, toDelete); err != nil {
			return fmt.Errorf("failed to delete rows: %w", err)
		}

		s.addDeletes(tableInfo.Name, len(toDelete))
	}

	return nil
}

// deleteAllInChunks deletes all rows from target table in chunks (by PK if available)
func (s *Syncer) deleteAllInChunks(ctx context.Context, tableInfo *table.Info) error {
	if s.cfg.DryRun {
		s.logger.Info("[DRY-RUN] Would delete all rows from table", "table", tableInfo.Name)
		return nil
	}

	if len(tableInfo.PrimaryKey) == 0 {
		// Fall back to a single DELETE FROM table
		query := fmt.Sprintf("DELETE FROM %s", s.quotedTableName(tableInfo.Name))
		res, err := s.targetDB.ExecContext(ctx, query)
		if err != nil {
			return fmt.Errorf("failed to delete all rows: %w", err)
		}
		if n, err := res.RowsAffected(); err == nil {
			s.addDeletes(tableInfo.Name, int(n))
		}
		return nil
	}

	pkCols := s.quotedColumnsList(tableInfo.PrimaryKey)
	batch := s.cfg.BatchSize
	if batch <= 0 {
		batch = constants.DefaultBatchSize
	}

	for {
		// Check for context cancellation
		if ctx.Err() != nil {
			return ctx.Err()
		}

		selectQuery := fmt.Sprintf("SELECT %s FROM %s LIMIT %d", pkCols, s.quotedTableName(tableInfo.Name), batch)
		rows, err := s.targetDB.QueryContext(ctx, selectQuery)
		if err != nil {
			return fmt.Errorf("failed to select rows for deletion: %w", err)
		}

		var pkRows [][]any
		for rows.Next() {
			values := make([]any, len(tableInfo.PrimaryKey))
			scanArgs := make([]any, len(tableInfo.PrimaryKey))
			for i := range values {
				scanArgs[i] = &values[i]
			}
			if err := rows.Scan(scanArgs...); err != nil {
				_ = rows.Close()
				return fmt.Errorf("failed to scan row: %w", err)
			}
			pkRows = append(pkRows, values)
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			return fmt.Errorf("error iterating rows: %w", err)
		}
		_ = rows.Close()

		if len(pkRows) == 0 {
			break
		}

		s.logger.Debug("Deleting rows", "table", tableInfo.Name, "count", len(pkRows))
		if err := s.deleteRows(ctx, tableInfo, pkRows); err != nil {
			return err
		}
		s.addDeletes(tableInfo.Name, len(pkRows))

		if len(pkRows) < batch {
			break
		}
	}
	return nil
}

// BulkDeleteBatchSize defines the number of rows per bulk DELETE statement
const BulkDeleteBatchSize = 100

// deleteRows deletes specified rows from target table using bulk operations
func (s *Syncer) deleteRows(ctx context.Context, tableInfo *table.Info, rows [][]any) error {
	if len(rows) == 0 {
		return nil
	}

	if s.cfg.DryRun {
		s.logger.Info("[DRY-RUN] Would delete rows", "table", tableInfo.Name, "count", len(rows))
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

	// For single-column PKs, use bulk DELETE with IN clause
	// For composite PKs, use prepared statement per row
	if len(tableInfo.PrimaryKey) == 1 {
		err = s.bulkDeleteSinglePK(ctx, tx, tableInfo, rows)
	} else {
		err = s.bulkDeleteCompositePK(ctx, tx, tableInfo, rows)
	}

	if err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// bulkDeleteSinglePK performs bulk DELETE using IN clause for single-column PK
func (s *Syncer) bulkDeleteSinglePK(ctx context.Context, tx *sql.Tx, tableInfo *table.Info, rows [][]any) error {
	pkCol := s.quotedColumnName(tableInfo.PrimaryKey[0])

	// Process in batches
	for i := 0; i < len(rows); i += BulkDeleteBatchSize {
		end := i + BulkDeleteBatchSize
		if end > len(rows) {
			end = len(rows)
		}
		batch := rows[i:end]

		// Build placeholders and collect args
		placeholders := make([]string, len(batch))
		args := make([]any, len(batch))
		for j, row := range batch {
			placeholders[j] = fmt.Sprintf("$%d", j+1)
			args[j] = row[0] // Single PK value
		}

		query := fmt.Sprintf("DELETE FROM %s WHERE %s IN (%s)",
			s.quotedTableName(tableInfo.Name),
			pkCol,
			strings.Join(placeholders, ", "))

		if _, err := tx.ExecContext(ctx, query, args...); err != nil {
			return fmt.Errorf("failed to bulk delete: %w", err)
		}
	}

	return nil
}

// bulkDeleteCompositePK performs DELETE using prepared statement for composite PKs
func (s *Syncer) bulkDeleteCompositePK(ctx context.Context, tx *sql.Tx, tableInfo *table.Info, rows [][]any) error {
	whereClause := s.buildPKWhereClause(tableInfo.PrimaryKey)
	deleteQuery := fmt.Sprintf("DELETE FROM %s WHERE %s", s.quotedTableName(tableInfo.Name), whereClause)

	stmt, err := tx.PrepareContext(ctx, deleteQuery)
	if err != nil {
		return fmt.Errorf("failed to prepare delete statement: %w", err)
	}
	defer func() { _ = stmt.Close() }()

	for _, row := range rows {
		if _, err := stmt.ExecContext(ctx, row...); err != nil {
			return fmt.Errorf("failed to delete row: %w", err)
		}
	}

	return nil
}

// handleMissingRows identifies and inserts rows that exist in source but not in target
// This handles the case where new rows were inserted with timestamps older than the max
func (s *Syncer) handleMissingRows(ctx context.Context, tableInfo *table.Info) error {
	if len(tableInfo.PrimaryKey) == 0 {
		s.logger.Debug("Table has no primary key, skipping missing rows handling", "table", tableInfo.Name)
		return nil
	}

	pkCols := s.quotedColumnsList(tableInfo.PrimaryKey)

	// Get all primary keys from target
	targetQuery := fmt.Sprintf("SELECT %s FROM %s", pkCols, s.quotedTableName(tableInfo.Name))
	targetRows, err := s.targetDB.QueryContext(ctx, targetQuery)
	if err != nil {
		return fmt.Errorf("failed to query target primary keys: %w", err)
	}
	defer func() { _ = targetRows.Close() }()

	targetPKs := make(map[string]bool)
	for targetRows.Next() {
		values := make([]any, len(tableInfo.PrimaryKey))
		scanArgs := make([]any, len(tableInfo.PrimaryKey))
		for i := range values {
			scanArgs[i] = &values[i]
		}

		if err := targetRows.Scan(scanArgs...); err != nil {
			return fmt.Errorf("failed to scan target primary key: %w", err)
		}

		pkStr := s.createPKString(values)
		targetPKs[pkStr] = true
	}

	if err := targetRows.Err(); err != nil {
		return fmt.Errorf("error iterating target rows: %w", err)
	}

	// Get all primary keys from source and identify missing rows
	sourceQuery := fmt.Sprintf("SELECT %s FROM %s", pkCols, s.quotedTableName(tableInfo.Name))
	sourceRows, err := s.sourceDB.QueryContext(ctx, sourceQuery)
	if err != nil {
		return fmt.Errorf("failed to query source primary keys: %w", err)
	}
	defer func() { _ = sourceRows.Close() }()

	var missingPKs [][]any
	for sourceRows.Next() {
		values := make([]any, len(tableInfo.PrimaryKey))
		scanArgs := make([]any, len(tableInfo.PrimaryKey))
		for i := range values {
			scanArgs[i] = &values[i]
		}

		if err := sourceRows.Scan(scanArgs...); err != nil {
			return fmt.Errorf("failed to scan source primary key: %w", err)
		}

		pkStr := s.createPKString(values)
		if !targetPKs[pkStr] {
			missingPKs = append(missingPKs, values)
		}
	}

	if err := sourceRows.Err(); err != nil {
		return fmt.Errorf("error iterating source rows: %w", err)
	}

	// Fetch and insert missing rows in batches
	if len(missingPKs) > 0 {
		s.logger.Debug("Found missing rows", "table", tableInfo.Name, "count", len(missingPKs))

		batchSize := s.cfg.BatchSize
		for i := 0; i < len(missingPKs); i += batchSize {
			end := i + batchSize
			if end > len(missingPKs) {
				end = len(missingPKs)
			}
			batch := missingPKs[i:end]

			// Fetch full rows from source
			rows, err := s.getRowsByPKValues(ctx, tableInfo, batch)
			if err != nil {
				return fmt.Errorf("failed to fetch missing rows: %w", err)
			}

			// Upsert to target
			if len(rows) > 0 {
				if err := s.upsertData(ctx, tableInfo, rows); err != nil {
					return fmt.Errorf("failed to upsert missing rows: %w", err)
				}
			}
		}
	}

	return nil
}

// getRowsByPKValues fetches full rows from source by their primary key values
func (s *Syncer) getRowsByPKValues(ctx context.Context, tableInfo *table.Info, pkValues [][]any) ([][]any, error) {
	if len(pkValues) == 0 {
		return nil, nil
	}

	columns := s.quotedColumnsList(tableInfo.Columns)

	// Build WHERE clause with OR conditions for each PK
	var conditions []string
	var args []any
	argIdx := 1

	for _, pk := range pkValues {
		var pkConds []string
		for i, col := range tableInfo.PrimaryKey {
			pkConds = append(pkConds, fmt.Sprintf(`"%s" = $%d`, col, argIdx))
			args = append(args, pk[i])
			argIdx++
		}
		conditions = append(conditions, "("+strings.Join(pkConds, " AND ")+")")
	}

	query := fmt.Sprintf("SELECT %s FROM %s WHERE %s",
		columns,
		s.quotedTableName(tableInfo.Name),
		strings.Join(conditions, " OR "),
	)

	rows, err := s.sourceDB.QueryContext(ctx, query, args...)
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
