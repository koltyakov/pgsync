package sync

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/koltyakov/pgsync/internal/constants"
	"github.com/koltyakov/pgsync/internal/table"
)

// syncTableIncremental performs incremental sync based on timestamp
func (s *Syncer) syncTableIncremental(ctx context.Context, tableInfo *table.Info, lastSync time.Time, timestampSource string) error {
	tableName := tableInfo.Name

	// Get the range of timestamps to process
	minTS, maxTS, err := s.getTimestampRange(ctx, tableName, lastSync)
	if err != nil {
		return fmt.Errorf("failed to get timestamp range: %w", err)
	}

	if minTS.IsZero() {
		s.logger.Debug("No new data found", "table", tableName)

		// Even when there's no new data, check if we should handle deletions
		sourceMaxTS, err := s.getSourceMaxTimestamp(ctx, tableName)
		if err != nil {
			return fmt.Errorf("failed to get source max timestamp: %w", err)
		}

		targetMaxTS, err := s.getMaxTimestampFromTarget(ctx, tableName)
		if err != nil {
			return fmt.Errorf("failed to get target max timestamp: %w", err)
		}

		// Special case: if source is empty and target has rows, delete all in target in chunks
		sourceHas, err := s.tableHasRows(ctx, s.sourceDB, tableName)
		if err != nil {
			return fmt.Errorf("failed to check source rows: %w", err)
		}
		targetHas, err := s.tableHasRows(ctx, s.targetDB, tableName)
		if err != nil {
			return fmt.Errorf("failed to check target rows: %w", err)
		}
		if !sourceHas && targetHas {
			s.logger.Debug("Source is empty, deleting all rows from target", "table", tableName)
			if err := s.deleteAllInChunks(ctx, tableInfo); err != nil {
				return fmt.Errorf("failed to delete all target rows: %w", err)
			}
			return nil
		}

		// Only handle deletions if target is caught up with source
		if !targetMaxTS.IsZero() && !sourceMaxTS.IsZero() && (targetMaxTS.Equal(sourceMaxTS) || targetMaxTS.After(sourceMaxTS)) {
			s.logger.Debug("Checking for deletions", "table", tableName)
			if err := s.handleDeletedRows(ctx, tableInfo); err != nil {
				return fmt.Errorf("failed to handle deleted rows: %w", err)
			}
		}

		return nil
	}

	s.logger.Debug("Syncing table",
		"table", tableName,
		"timestampSource", timestampSource,
		"lastSync", lastSync.Format(time.RFC3339),
		"fromTimestamp", minTS.Format(time.RFC3339),
	)

	// Process data in batches
	currentTS := minTS
	for currentTS.Before(maxTS) || currentTS.Equal(maxTS) {
		// Check for context cancellation
		if ctx.Err() != nil {
			return ctx.Err()
		}

		nextTS, err := s.processBatch(ctx, tableInfo, currentTS, maxTS)
		if err != nil {
			return fmt.Errorf("failed to process batch: %w", err)
		}

		currentTS = nextTS.Add(time.Microsecond) // Move slightly forward to avoid duplicate processing
	}

	// After successful incremental sync, check if we should handle deletions
	sourceMaxTS, err := s.getSourceMaxTimestamp(ctx, tableName)
	if err != nil {
		return fmt.Errorf("failed to get source max timestamp: %w", err)
	}

	targetMaxTS, err := s.getMaxTimestampFromTarget(ctx, tableName)
	if err != nil {
		return fmt.Errorf("failed to get target max timestamp: %w", err)
	}

	// Only handle deletions if target is caught up with source
	if !targetMaxTS.IsZero() && !sourceMaxTS.IsZero() && (targetMaxTS.Equal(sourceMaxTS) || targetMaxTS.After(sourceMaxTS)) {
		s.logger.Debug("Sync completed, checking for deletions", "table", tableName)
		if err := s.handleDeletedRows(ctx, tableInfo); err != nil {
			return fmt.Errorf("failed to handle deleted rows: %w", err)
		}
	}

	return nil
}

// getTimestampRange gets the range of timestamps to process
func (s *Syncer) getTimestampRange(ctx context.Context, tableName string, lastSync time.Time) (time.Time, time.Time, error) {
	query := fmt.Sprintf(`
		SELECT MIN(%s) as min_ts, MAX(%s) as max_ts 
		FROM %s 
		WHERE %s > $1`,
		s.quotedColumnName(s.cfg.TimestampCol), s.quotedColumnName(s.cfg.TimestampCol),
		s.quotedTableName(tableName), s.quotedColumnName(s.cfg.TimestampCol))

	var minTS, maxTS sql.NullTime
	err := s.sourceDB.QueryRowContext(ctx, query, lastSync).Scan(&minTS, &maxTS)
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("failed to query timestamp range: %w", err)
	}

	if !minTS.Valid || !maxTS.Valid {
		return time.Time{}, time.Time{}, nil // No new data
	}

	return minTS.Time, maxTS.Time, nil
}

// getMaxTimestampFromTarget gets the maximum timestamp from target table
func (s *Syncer) getMaxTimestampFromTarget(ctx context.Context, tableName string) (time.Time, error) {
	query := fmt.Sprintf(`
		SELECT COALESCE(MAX(%s), '1970-01-01'::timestamp) 
		FROM %s`,
		s.quotedColumnName(s.cfg.TimestampCol), s.quotedTableName(tableName))

	var maxTS time.Time
	err := s.targetDB.QueryRowContext(ctx, query).Scan(&maxTS)
	if err != nil {
		// If table doesn't exist in target, return zero time
		if strings.Contains(err.Error(), "does not exist") || strings.Contains(err.Error(), "relation") {
			return time.Time{}, nil
		}
		return time.Time{}, fmt.Errorf("failed to get max timestamp from target %s: %w", tableName, err)
	}

	// If the result is the epoch time (1970-01-01), treat it as zero
	epoch := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
	if maxTS.Equal(epoch) {
		return time.Time{}, nil
	}

	return maxTS, nil
}

// getSourceMaxTimestamp gets the maximum timestamp from source table
func (s *Syncer) getSourceMaxTimestamp(ctx context.Context, tableName string) (time.Time, error) {
	query := fmt.Sprintf(`
		SELECT COALESCE(MAX(%s), '1970-01-01'::timestamp) 
		FROM %s`,
		s.quotedColumnName(s.cfg.TimestampCol), s.quotedTableName(tableName))

	var maxTS time.Time
	err := s.sourceDB.QueryRowContext(ctx, query).Scan(&maxTS)
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to get max timestamp from source %s: %w", tableName, err)
	}

	// If the result is the epoch time (1970-01-01), treat it as zero
	epoch := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
	if maxTS.Equal(epoch) {
		return time.Time{}, nil
	}

	return maxTS, nil
}

// processBatch processes a batch of data for a given timestamp range
func (s *Syncer) processBatch(ctx context.Context, tableInfo *table.Info, fromTS, maxTS time.Time) (time.Time, error) {
	// Calculate the batch end timestamp
	batchEndTS := s.calculateBatchEndTimestamp(ctx, tableInfo, fromTS, maxTS)

	// Ensure we get all rows with the same timestamp as batchEndTS
	actualEndTS, err := s.getActualBatchEndTimestamp(ctx, tableInfo, fromTS, batchEndTS)
	if err != nil {
		return time.Time{}, err
	}

	s.logger.Debug("Processing batch", "table", tableInfo.Name, "from", fromTS.Format(time.RFC3339))

	// Get data from source
	rows, err := s.getSourceData(ctx, tableInfo, fromTS, actualEndTS)
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to get source data: %w", err)
	}

	if len(rows) == 0 {
		return actualEndTS, nil
	}

	// Upsert data to target (respects dry-run mode)
	if err := s.upsertData(ctx, tableInfo, rows); err != nil {
		return time.Time{}, fmt.Errorf("failed to upsert data: %w", err)
	}

	return actualEndTS, nil
}

// calculateBatchEndTimestamp calculates the end timestamp for the current batch
func (s *Syncer) calculateBatchEndTimestamp(ctx context.Context, tableInfo *table.Info, fromTS, maxTS time.Time) time.Time {
	query := fmt.Sprintf(`
		SELECT %s 
		FROM %s 
		WHERE %s >= $1 AND %s <= $2 
		ORDER BY %s 
		LIMIT $3`,
		s.quotedColumnName(s.cfg.TimestampCol), s.quotedTableName(tableInfo.Name),
		s.quotedColumnName(s.cfg.TimestampCol), s.quotedColumnName(s.cfg.TimestampCol),
		s.quotedColumnName(s.cfg.TimestampCol))

	rows, err := s.sourceDB.QueryContext(ctx, query, fromTS, maxTS, s.cfg.BatchSize)
	if err != nil {
		s.logger.Warn("Failed to calculate batch end timestamp, using max", "table", tableInfo.Name, "error", err)
		return maxTS
	}
	defer func() { _ = rows.Close() }()

	var lastTS time.Time
	count := 0
	for rows.Next() {
		count++
		if err := rows.Scan(&lastTS); err != nil {
			s.logger.Warn("Failed to scan timestamp, using max", "table", tableInfo.Name, "error", err)
			return maxTS
		}
	}

	if err := rows.Err(); err != nil {
		s.logger.Warn("Error iterating rows, using max", "table", tableInfo.Name, "error", err)
		return maxTS
	}

	if count < s.cfg.BatchSize {
		return maxTS // We got all remaining data
	}

	return lastTS
}

// getActualBatchEndTimestamp ensures we include all rows with the same timestamp
func (s *Syncer) getActualBatchEndTimestamp(ctx context.Context, tableInfo *table.Info, fromTS, batchEndTS time.Time) (time.Time, error) {
	query := fmt.Sprintf(`
		SELECT MIN(%s) 
		FROM %s 
		WHERE %s > $1`,
		s.quotedColumnName(s.cfg.TimestampCol), s.quotedTableName(tableInfo.Name),
		s.quotedColumnName(s.cfg.TimestampCol))

	var nextTS sql.NullTime
	err := s.sourceDB.QueryRowContext(ctx, query, batchEndTS).Scan(&nextTS)
	if err != nil {
		return batchEndTS, nil // Use the original batch end
	}

	if !nextTS.Valid {
		return batchEndTS, nil // No more data after this timestamp
	}

	// Return the timestamp just before the next one
	return nextTS.Time.Add(-time.Microsecond), nil
}

// getSourceData retrieves data from source table within timestamp range
func (s *Syncer) getSourceData(ctx context.Context, tableInfo *table.Info, fromTS, toTS time.Time) ([][]any, error) {
	columns := s.quotedColumnsList(tableInfo.Columns)
	query := fmt.Sprintf(`
		SELECT %s 
		FROM %s 
		WHERE %s >= $1 AND %s <= $2 
		ORDER BY %s`,
		columns, s.quotedTableName(tableInfo.Name),
		s.quotedColumnName(s.cfg.TimestampCol), s.quotedColumnName(s.cfg.TimestampCol),
		s.quotedColumnName(s.cfg.TimestampCol))

	rows, err := s.sourceDB.QueryContext(ctx, query, fromTS, toTS)
	if err != nil {
		return nil, fmt.Errorf("failed to query source data: %w", err)
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

// tableHasRows checks if a table has at least one row
func (s *Syncer) tableHasRows(ctx context.Context, dbh *sql.DB, tableName string) (bool, error) {
	query := fmt.Sprintf("SELECT EXISTS (SELECT 1 FROM %s LIMIT 1)", s.quotedTableName(tableName))
	var exists bool
	if err := dbh.QueryRowContext(ctx, query).Scan(&exists); err != nil {
		return false, fmt.Errorf("failed to check if table has rows: %w", err)
	}
	return exists, nil
}

// syncTable synchronizes a single table
func (s *Syncer) syncTable(ctx context.Context, tableInfo *table.Info) error {
	tableName := tableInfo.Name

	// Capture baseline counts and always log per-table delta at exit
	preUp, preDel := s.tableCounts(tableName)
	defer func() {
		postUp, postDel := s.tableCounts(tableName)
		s.logger.Info("Table sync completed",
			"table", tableName,
			"synced", postUp-preUp,
			"deleted", postDel-preDel,
		)
	}()

	// Reconcile mode: full comparison by primary key, ignore timestamps
	if s.cfg.Reconcile {
		if len(tableInfo.PrimaryKey) == 0 {
			s.logger.Debug("Table has no primary key, skipping reconciliation",
				"table", tableName,
			)
			s.addSkipped(tableName)
			return nil
		}
		s.logger.Debug("Reconciliation mode: comparing all rows by primary key",
			"table", tableName,
			"rowCount", tableInfo.RowCount,
		)
		return s.syncTableReconcile(ctx, tableInfo)
	}

	// Check if table has timestamp column
	hasTimestamp := tableInfo.HasColumn(s.cfg.TimestampCol)

	if !hasTimestamp {
		// For small tables (<SmallTableThreshold rows) with primary key, perform full upsert + deletion check
		if len(tableInfo.PrimaryKey) == 0 {
			s.logger.Debug("Table has no timestamp column and no primary key, skipping",
				"table", tableName,
				"timestampColumn", s.cfg.TimestampCol,
			)
			return nil
		}
		if tableInfo.RowCount <= constants.SmallTableThreshold {
			s.logger.Debug("Table has no timestamp column, performing full diff",
				"table", tableName,
				"timestampColumn", s.cfg.TimestampCol,
				"rowCount", tableInfo.RowCount,
			)
			return s.syncTableFullSmall(ctx, tableInfo)
		}
		s.logger.Info("Table has no timestamp column and is too large, skipping full sync",
			"table", tableName,
			"timestampColumn", s.cfg.TimestampCol,
			"rowCount", tableInfo.RowCount,
		)
		s.addSkipped(tableName)
		return nil
	}

	// Always prefer timestamp from target DB for determining sync point
	targetMaxTimestamp, err := s.getMaxTimestampFromTarget(ctx, tableName)
	if err != nil {
		return fmt.Errorf("failed to get max timestamp from target: %w", err)
	}

	var lastSync time.Time
	var timestampSource string
	if !targetMaxTimestamp.IsZero() {
		lastSync = targetMaxTimestamp
		timestampSource = "target DB"
	} else {
		lastSync = time.Time{}
		timestampSource = "zero time (empty target)"
	}
	return s.syncTableIncremental(ctx, tableInfo, lastSync, timestampSource)
}
