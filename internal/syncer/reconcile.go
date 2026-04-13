package syncer

import (
	"context"
	"fmt"

	"github.com/koltyakov/pgsync/internal/table"
)

func (s *Syncer) syncTableReconcile(ctx context.Context, tableInfo *table.Info) error {
	tableName := tableInfo.Name

	diff, err := s.diffPKs(ctx, tableInfo)
	if err != nil {
		return fmt.Errorf("failed to diff PKs: %w", err)
	}

	totalSource := len(diff.missing) + len(diff.extra)
	s.logger.Debug("Reconciliation: comparing primary keys",
		"table", tableName,
		"totalPKs", totalSource,
		"missing", len(diff.missing),
		"extra", len(diff.extra),
	)

	if len(diff.missing) > 0 {
		missingPKs := make([][]any, 0, len(diff.missing))
		for _, vals := range diff.missing {
			missingPKs = append(missingPKs, vals)
		}

		batchSize := s.cfg.BatchSize
		for i := 0; i < len(missingPKs); i += batchSize {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			end := i + batchSize
			if end > len(missingPKs) {
				end = len(missingPKs)
			}
			batch := missingPKs[i:end]

			rows, err := s.fetchRowsByPK(ctx, s.sourceDB, tableInfo, batch)
			if err != nil {
				return fmt.Errorf("failed to fetch source rows: %w", err)
			}

			if len(rows) > 0 {
				if err := s.upsertData(ctx, tableInfo, rows); err != nil {
					return fmt.Errorf("failed to upsert rows: %w", err)
				}
			}

			s.logger.Debug("Reconciliation: synced batch",
				"table", tableName,
				"batch", len(batch),
				"progress", fmt.Sprintf("%d/%d", end, len(missingPKs)),
			)
		}
	}

	if len(diff.extra) > 0 {
		extraPKs := make([][]any, 0, len(diff.extra))
		for _, vals := range diff.extra {
			extraPKs = append(extraPKs, vals)
		}

		batchSize := s.cfg.BatchSize
		for i := 0; i < len(extraPKs); i += batchSize {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			end := i + batchSize
			if end > len(extraPKs) {
				end = len(extraPKs)
			}
			batch := extraPKs[i:end]

			if err := s.deleteRows(ctx, tableInfo, batch); err != nil {
				return fmt.Errorf("failed to delete rows: %w", err)
			}
			s.addDeletes(tableName, len(batch))

			s.logger.Debug("Reconciliation: deleted batch",
				"table", tableName,
				"batch", len(batch),
				"progress", fmt.Sprintf("%d/%d", end, len(extraPKs)),
			)
		}
	}

	return nil
}
