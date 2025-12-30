package sync

import (
	"context"
	"fmt"
	"reflect"

	"github.com/koltyakov/pgsync/internal/table"
)

// syncTableFullSmall performs a full-table sync for small tables without a timestamp column
func (s *Syncer) syncTableFullSmall(ctx context.Context, tableInfo *table.Info) error {
	// Fetch all rows from source and target
	srcRows, err := s.getAllSourceData(ctx, tableInfo)
	if err != nil {
		return fmt.Errorf("failed to fetch full data from source: %w", err)
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

// getAllSourceData retrieves all rows for a table (no timestamp filter)
func (s *Syncer) getAllSourceData(ctx context.Context, tableInfo *table.Info) ([][]any, error) {
	columns := s.quotedColumnsList(tableInfo.Columns)
	query := fmt.Sprintf("SELECT %s FROM %s", columns, s.quotedTableName(tableInfo.Name))

	rows, err := s.sourceDB.QueryContext(ctx, query)
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

// getAllTargetData retrieves all rows from the target table
func (s *Syncer) getAllTargetData(ctx context.Context, tableInfo *table.Info) ([][]any, error) {
	columns := s.quotedColumnsList(tableInfo.Columns)
	query := fmt.Sprintf("SELECT %s FROM %s", columns, s.quotedTableName(tableInfo.Name))

	rows, err := s.targetDB.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query target data: %w", err)
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

// rowsEqual compares two database rows by value
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
