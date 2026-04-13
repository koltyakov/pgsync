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
}

func (s *Syncer) diffPKs(ctx context.Context, tableInfo *table.Info) (*pkDiff, error) {
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
	for pk, vals := range sourcePKs {
		if _, exists := targetPKs[pk]; !exists {
			missing[pk] = vals
		}
	}

	extra := make(map[string][]any)
	for pk, vals := range targetPKs {
		if _, exists := sourcePKs[pk]; !exists {
			extra[pk] = vals
		}
	}

	return &pkDiff{missing: missing, extra: extra}, nil
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

	diff, err := s.diffPKs(ctx, tableInfo)
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
