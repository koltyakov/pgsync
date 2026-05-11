package syncer

import (
	"context"
	"database/sql"
	"fmt"
	"sync"

	"github.com/koltyakov/pgsync/internal/table"
)

type pkDiff struct {
	missing map[string][]any
	extra   map[string][]any
	common  map[string][]any
}

func (s *Syncer) diffPKs(ctx context.Context, tableInfo *table.Info, includeCommon bool) (*pkDiff, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	var sourcePKs map[string][]any
	var targetPKs map[string][]any
	var sourceErr, targetErr error
	var wg sync.WaitGroup

	wg.Add(2)

	go func() {
		defer wg.Done()
		sourcePKs, sourceErr = s.fetchPKs(ctx, s.sourceDB, tableInfo)
	}()

	go func() {
		defer wg.Done()
		targetPKs, targetErr = s.fetchPKs(ctx, s.targetDB, tableInfo)
	}()

	wg.Wait()

	if sourceErr != nil {
		return nil, fmt.Errorf("failed to fetch source PKs: %w", sourceErr)
	}
	if targetErr != nil {
		return nil, fmt.Errorf("failed to fetch target PKs: %w", targetErr)
	}

	missing := make(map[string][]any)
	var common map[string][]any
	if includeCommon {
		common = make(map[string][]any)
	}
	for pk, vals := range sourcePKs {
		if _, exists := targetPKs[pk]; exists {
			if includeCommon {
				common[pk] = vals
			}
		} else {
			missing[pk] = vals
		}
	}

	extra := make(map[string][]any)
	for pk, vals := range targetPKs {
		if _, exists := sourcePKs[pk]; !exists {
			extra[pk] = vals
		}
	}

	return &pkDiff{missing: missing, extra: extra, common: common}, nil
}

func (s *Syncer) fetchPKs(ctx context.Context, db *sql.DB, tableInfo *table.Info) (map[string][]any, error) {
	pkCols := s.quotedColumnsList(tableInfo.PrimaryKey)
	query := fmt.Sprintf("SELECT %s FROM %s", pkCols, s.quotedTableName(tableInfo.Name)) //nolint:gosec // G201 - table/column names are safely quoted

	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query PKs: %w", err)
	}
	defer func() { _ = rows.Close() }()

	result := make(map[string][]any)
	for rows.Next() {
		pkVals := make([]any, len(tableInfo.PrimaryKey))
		scanArgs := make([]any, len(tableInfo.PrimaryKey))
		for i := range pkVals {
			scanArgs[i] = &pkVals[i]
		}
		if err := rows.Scan(scanArgs...); err != nil {
			return nil, fmt.Errorf("failed to scan PK: %w", err)
		}
		pkStr := s.createPKString(pkVals)
		result[pkStr] = pkVals
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating PKs: %w", err)
	}

	return result, nil
}

func (s *Syncer) handleRowDifferences(ctx context.Context, tableInfo *table.Info) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if len(tableInfo.PrimaryKey) == 0 {
		s.logger.Debug("Table has no primary key, skipping row diff", "table", tableInfo.Name)
		return nil
	}

	diff, err := s.diffPKs(ctx, tableInfo, false)
	if err != nil {
		return err
	}

	if len(diff.extra) > 0 {
		extraPKs := make([][]any, 0, len(diff.extra))
		for _, vals := range diff.extra {
			extraPKs = append(extraPKs, vals)
		}
		batchSize := s.cfg.BatchSize
		for i := 0; i < len(extraPKs); i += batchSize {
			end := i + batchSize
			if end > len(extraPKs) {
				end = len(extraPKs)
			}
			batch := extraPKs[i:end]
			s.logger.Debug("Deleting extra rows", "table", tableInfo.Name, "count", len(batch))
			if err := s.deleteRows(ctx, tableInfo, batch); err != nil {
				return fmt.Errorf("failed to delete extra rows: %w", err)
			}
			s.addDeletes(tableInfo.Name, len(batch))
		}
	}

	if len(diff.missing) > 0 {
		missingPKs := make([][]any, 0, len(diff.missing))
		for _, vals := range diff.missing {
			missingPKs = append(missingPKs, vals)
		}
		s.logger.Debug("Found missing rows", "table", tableInfo.Name, "count", len(missingPKs))

		batchSize := s.cfg.BatchSize
		for i := 0; i < len(missingPKs); i += batchSize {
			end := i + batchSize
			if end > len(missingPKs) {
				end = len(missingPKs)
			}
			batch := missingPKs[i:end]

			rows, err := s.fetchRowsByPK(ctx, s.sourceDB, tableInfo, batch)
			if err != nil {
				return fmt.Errorf("failed to fetch missing rows: %w", err)
			}

			if len(rows) > 0 {
				if err := s.upsertData(ctx, tableInfo, rows); err != nil {
					return fmt.Errorf("failed to upsert missing rows: %w", err)
				}
			}
		}
	}

	return nil
}

func (s *Syncer) syncChangedRows(ctx context.Context, tableInfo *table.Info, commonPKs map[string][]any) (int, error) {
	if len(commonPKs) == 0 {
		return 0, nil
	}

	pkValues := make([][]any, 0, len(commonPKs))
	for _, vals := range commonPKs {
		pkValues = append(pkValues, vals)
	}

	batchSize := s.cfg.BatchSize
	changedCount := 0
	for i := 0; i < len(pkValues); i += batchSize {
		if ctx.Err() != nil {
			return changedCount, ctx.Err()
		}

		end := i + batchSize
		if end > len(pkValues) {
			end = len(pkValues)
		}
		batch := pkValues[i:end]

		sourceRows, err := s.fetchRowsByPK(ctx, s.sourceDB, tableInfo, batch)
		if err != nil {
			return changedCount, fmt.Errorf("failed to fetch source rows for comparison: %w", err)
		}
		targetRows, err := s.fetchRowsByPK(ctx, s.targetDB, tableInfo, batch)
		if err != nil {
			return changedCount, fmt.Errorf("failed to fetch target rows for comparison: %w", err)
		}

		changedRows, err := s.changedRows(tableInfo, sourceRows, targetRows)
		if err != nil {
			return changedCount, err
		}
		if len(changedRows) == 0 {
			continue
		}

		if err := s.upsertData(ctx, tableInfo, changedRows); err != nil {
			return changedCount, fmt.Errorf("failed to upsert changed rows: %w", err)
		}
		changedCount += len(changedRows)
	}

	return changedCount, nil
}

func (s *Syncer) changedRows(tableInfo *table.Info, sourceRows, targetRows [][]any) ([][]any, error) {
	colIdx, err := columnIndexForPK(tableInfo)
	if err != nil {
		return nil, err
	}

	targetByPK := make(map[string][]any, len(targetRows))
	for _, row := range targetRows {
		pk, err := s.pkStringFromRow(tableInfo, row, colIdx)
		if err != nil {
			return nil, err
		}
		targetByPK[pk] = row
	}

	changed := make([][]any, 0)
	for _, row := range sourceRows {
		pk, err := s.pkStringFromRow(tableInfo, row, colIdx)
		if err != nil {
			return nil, err
		}
		targetRow, ok := targetByPK[pk]
		if !ok || !rowsEqual(row, targetRow) {
			changed = append(changed, row)
		}
	}

	return changed, nil
}

func columnIndexForPK(tableInfo *table.Info) (map[string]int, error) {
	if tableInfo == nil {
		return nil, fmt.Errorf("tableInfo is nil")
	}

	colIdx := make(map[string]int, len(tableInfo.Columns))
	for i, col := range tableInfo.Columns {
		colIdx[col] = i
	}
	for _, pk := range tableInfo.PrimaryKey {
		if _, ok := colIdx[pk]; !ok {
			return nil, fmt.Errorf("primary key column %q not found in table %q columns", pk, tableInfo.Name)
		}
	}

	return colIdx, nil
}

func (s *Syncer) pkStringFromRow(tableInfo *table.Info, row []any, colIdx map[string]int) (string, error) {
	pkVals := make([]any, len(tableInfo.PrimaryKey))
	for i, pk := range tableInfo.PrimaryKey {
		idx := colIdx[pk]
		if idx >= len(row) {
			return "", fmt.Errorf("row for table %q has %d columns, primary key %q needs index %d", tableInfo.Name, len(row), pk, idx)
		}
		pkVals[i] = row[idx]
	}
	return s.createPKString(pkVals), nil
}
