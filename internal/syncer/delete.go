// Package syncer provides PostgreSQL table synchronization with incremental and full sync modes.
package syncer

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/koltyakov/pgsync/internal/constants"
	"github.com/koltyakov/pgsync/internal/table"
)

func (s *Syncer) deleteAllInChunks(ctx context.Context, tableInfo *table.Info) error {
	if s.cfg.DryRun {
		s.logger.Info("[DRY-RUN] Would delete all rows from table", "table", tableInfo.Name)
		return nil
	}

	if len(tableInfo.PrimaryKey) == 0 {
		query := fmt.Sprintf("DELETE FROM %s", s.quotedTableName(tableInfo.Name)) //nolint:gosec // G201 - table name is safely quoted
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
		if ctx.Err() != nil {
			return ctx.Err()
		}

		selectQuery := fmt.Sprintf("SELECT %s FROM %s LIMIT %d", pkCols, s.quotedTableName(tableInfo.Name), batch) //nolint:gosec // G201 - table/column names are safely quoted
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

// BulkDeleteBatchSize defines the number of rows per bulk DELETE statement.
const BulkDeleteBatchSize = 500

func (s *Syncer) deleteRows(ctx context.Context, tableInfo *table.Info, rows [][]any) error {
	if len(rows) == 0 {
		return nil
	}

	if s.cfg.DryRun {
		s.logger.Info("[DRY-RUN] Would delete rows", "table", tableInfo.Name, "count", len(rows))
		return nil
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

func (s *Syncer) bulkDeleteSinglePK(ctx context.Context, tx *sql.Tx, tableInfo *table.Info, rows [][]any) error {
	pkCol := s.quotedColumnName(tableInfo.PrimaryKey[0])

	for i := 0; i < len(rows); i += BulkDeleteBatchSize {
		end := i + BulkDeleteBatchSize
		if end > len(rows) {
			end = len(rows)
		}
		batch := rows[i:end]

		placeholders := make([]string, len(batch))
		args := make([]any, len(batch))
		for j, row := range batch {
			placeholders[j] = fmt.Sprintf("$%d", j+1)
			args[j] = row[0]
		}

		query := fmt.Sprintf("DELETE FROM %s WHERE %s IN (%s)", //nolint:gosec // G201 - table/column names are safely quoted
			s.quotedTableName(tableInfo.Name),
			pkCol,
			strings.Join(placeholders, ", "))

		if _, err := tx.ExecContext(ctx, query, args...); err != nil {
			return fmt.Errorf("failed to bulk delete: %w", err)
		}
	}

	return nil
}

func (s *Syncer) bulkDeleteCompositePK(ctx context.Context, tx *sql.Tx, tableInfo *table.Info, rows [][]any) error {
	whereClause := s.buildPKWhereClause(tableInfo.PrimaryKey)
	deleteQuery := fmt.Sprintf("DELETE FROM %s WHERE %s", s.quotedTableName(tableInfo.Name), whereClause) //nolint:gosec // G201 - table/column names are safely quoted

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
