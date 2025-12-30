package sync

import (
	"context"
	"fmt"
	"reflect"

	"github.com/koltyakov/pgsync/internal/table"
)

// syncTableFullSmall performs a full-table sync for small tables without a timestamp column.
// This function is designed for tables with RowCount <= SmallTableThreshold.
// Algorithm:
//  1. Fetch all rows from source and target
//  2. Build PK-indexed maps for O(1) lookup
//  3. Compare rows to identify inserts, updates, and deletes
//  4. Apply changes respecting dry-run mode
func (s *Syncer) syncTableFullSmall(ctx context.Context, tableInfo *table.Info) error {
	// Defensive: validate input
	if tableInfo == nil {
		return fmt.Errorf("tableInfo is nil")
	}
	if len(tableInfo.Columns) == 0 {
		return fmt.Errorf("table %q has no columns", tableInfo.Name)
	}
	if len(tableInfo.PrimaryKey) == 0 {
		return fmt.Errorf("table %q has no primary key", tableInfo.Name)
	}

	// Check for context cancellation before expensive operations
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("context cancelled before full sync: %w", err)
	}

	// Fetch all rows from source and target
	srcRows, err := s.getAllSourceData(ctx, tableInfo)
	if err != nil {
		return fmt.Errorf("failed to fetch full data from source: %w", err)
	}

	// Check context between expensive operations
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("context cancelled after source fetch: %w", err)
	}

	tgtRows, err := s.getAllTargetData(ctx, tableInfo)
	if err != nil {
		return fmt.Errorf("failed to fetch full data from target: %w", err)
	}

	// Build column index map for PK extraction
	colIdx := make(map[string]int, len(tableInfo.Columns))
	for i, c := range tableInfo.Columns {
		colIdx[c] = i
	}

	// Validate PK columns exist in column list
	for _, pk := range tableInfo.PrimaryKey {
		if _, ok := colIdx[pk]; !ok {
			return fmt.Errorf("primary key column %q not found in table %q columns", pk, tableInfo.Name)
		}
	}

	// Index rows by PK composite key
	srcMap := make(map[string][]any, len(srcRows))
	for _, r := range srcRows {
		pkVals := make([]any, len(tableInfo.PrimaryKey))
		for i, pk := range tableInfo.PrimaryKey {
			pkVals[i] = r[colIdx[pk]]
		}
		srcMap[s.createPKString(pkVals)] = r
	}

	tgtMap := make(map[string][]any, len(tgtRows))
	for _, r := range tgtRows {
		pkVals := make([]any, len(tableInfo.PrimaryKey))
		for i, pk := range tableInfo.PrimaryKey {
			pkVals[i] = r[colIdx[pk]]
		}
		tgtMap[s.createPKString(pkVals)] = r
	}

	// Determine changed/inserted rows and deletions
	var toUpsert [][]any
	for pk, srow := range srcMap {
		if trow, ok := tgtMap[pk]; !ok {
			toUpsert = append(toUpsert, srow) // insert
		} else if !rowsEqual(srow, trow) {
			toUpsert = append(toUpsert, srow) // update
		}
	}

	var toDelete [][]any
	for pk, trow := range tgtMap {
		if _, ok := srcMap[pk]; !ok {
			// Extract PK values in order for delete
			pkVals := make([]any, len(tableInfo.PrimaryKey))
			for i, pkName := range tableInfo.PrimaryKey {
				pkVals[i] = trow[colIdx[pkName]]
			}
			toDelete = append(toDelete, pkVals)
		}
	}

	// Apply changes (respects dry-run mode)
	if len(toUpsert) > 0 {
		if err := s.upsertData(ctx, tableInfo, toUpsert); err != nil {
			return fmt.Errorf("failed to upsert changed rows: %w", err)
		}
	}
	if len(toDelete) > 0 {
		if err := s.deleteRows(ctx, tableInfo, toDelete); err != nil {
			return fmt.Errorf("failed to delete rows: %w", err)
		}
		s.addDeletes(tableInfo.Name, len(toDelete))
	}

	// Verbose summary
	if len(toUpsert) == 0 && len(toDelete) == 0 {
		s.logger.Debug("Small-table diff - no changes", "table", tableInfo.Name)
	} else {
		s.logger.Debug("Small-table diff completed",
			"table", tableInfo.Name,
			"changed", len(toUpsert),
			"deleted", len(toDelete),
		)
	}

	return nil
}

// getAllSourceData retrieves all rows for a table (no timestamp filter).
// Returns (rows, error) where rows is a slice of column values in tableInfo.Columns order.
// Caller must validate tableInfo is not nil and has columns.
func (s *Syncer) getAllSourceData(ctx context.Context, tableInfo *table.Info) ([][]any, error) {
	if s.sourceDB == nil {
		return nil, fmt.Errorf("source database connection is nil")
	}

	columns := s.quotedColumnsList(tableInfo.Columns)
	query := fmt.Sprintf("SELECT %s FROM %s", columns, s.quotedTableName(tableInfo.Name))

	rows, err := s.sourceDB.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query source data: %w", err)
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			s.logger.Warn("failed to close source rows", "table", tableInfo.Name, "error", closeErr)
		}
	}()

	// Pre-allocate with estimate to reduce allocations
	result := make([][]any, 0, tableInfo.RowCount)
	for rows.Next() {
		// Check context periodically during large scans
		if len(result)%10000 == 0 {
			if err := ctx.Err(); err != nil {
				return nil, fmt.Errorf("context cancelled during source scan: %w", err)
			}
		}

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

// getAllTargetData retrieves all rows from the target table.
// Returns (rows, error) where rows is a slice of column values in tableInfo.Columns order.
func (s *Syncer) getAllTargetData(ctx context.Context, tableInfo *table.Info) ([][]any, error) {
	if s.targetDB == nil {
		return nil, fmt.Errorf("target database connection is nil")
	}

	columns := s.quotedColumnsList(tableInfo.Columns)
	query := fmt.Sprintf("SELECT %s FROM %s", columns, s.quotedTableName(tableInfo.Name))

	rows, err := s.targetDB.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query target data: %w", err)
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			s.logger.Warn("failed to close target rows", "table", tableInfo.Name, "error", closeErr)
		}
	}()

	// Pre-allocate with reasonable estimate
	result := make([][]any, 0, tableInfo.RowCount)
	for rows.Next() {
		// Check context periodically during large scans
		if len(result)%10000 == 0 {
			if err := ctx.Err(); err != nil {
				return nil, fmt.Errorf("context cancelled during target scan: %w", err)
			}
		}

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

// rowsEqual compares two database rows by value.
// Handles nil values and uses deep equality for complex types.
// Returns false if slices have different lengths.
func rowsEqual(a, b []any) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !reflect.DeepEqual(a[i], b[i]) {
			return false
		}
	}
	return true
}
