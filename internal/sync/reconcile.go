package sync

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/koltyakov/pgsync/internal/table"
)

// syncTableReconcile performs a full reconciliation by comparing primary keys
// and syncing only the rows that differ. This is optimized for large tables.
func (s *Syncer) syncTableReconcile(ctx context.Context, tableInfo *table.Info) error {
	tableName := tableInfo.Name

	// Step 1: Get all PKs from source and target
	sourcePKs, err := s.getAllPrimaryKeys(ctx, s.sourceDB, tableInfo)
	if err != nil {
		return fmt.Errorf("failed to get source PKs: %w", err)
	}

	targetPKs, err := s.getAllPrimaryKeys(ctx, s.targetDB, tableInfo)
	if err != nil {
		return fmt.Errorf("failed to get target PKs: %w", err)
	}

	s.logger.Debug("Reconciliation: comparing primary keys",
		"table", tableName,
		"sourcePKs", len(sourcePKs),
		"targetPKs", len(targetPKs),
	)

	// Step 2: Find missing PKs (in source but not in target)
	missingPKs := make([]string, 0)
	for pk := range sourcePKs {
		if _, exists := targetPKs[pk]; !exists {
			missingPKs = append(missingPKs, pk)
		}
	}

	// Step 3: Find extra PKs (in target but not in source) - these need to be deleted
	extraPKs := make([]string, 0)
	for pk := range targetPKs {
		if _, exists := sourcePKs[pk]; !exists {
			extraPKs = append(extraPKs, pk)
		}
	}

	s.logger.Debug("Reconciliation: differences found",
		"table", tableName,
		"missing", len(missingPKs),
		"extra", len(extraPKs),
	)

	// Step 4: Fetch and upsert missing rows in batches
	if len(missingPKs) > 0 {
		if err := s.syncMissingRows(ctx, tableInfo, missingPKs, sourcePKs); err != nil {
			return fmt.Errorf("failed to sync missing rows: %w", err)
		}
	}

	// Step 5: Delete extra rows
	if len(extraPKs) > 0 {
		if err := s.deleteExtraRows(ctx, tableInfo, extraPKs, targetPKs); err != nil {
			return fmt.Errorf("failed to delete extra rows: %w", err)
		}
		s.addDeletes(tableName, len(extraPKs))
	}

	return nil
}

// getAllPrimaryKeys retrieves all primary key values from a database
// Returns a map of PK string -> slice of actual PK values
func (s *Syncer) getAllPrimaryKeys(ctx context.Context, db *sql.DB, tableInfo *table.Info) (map[string][]any, error) {
	pkColumns := s.quotedColumnsList(tableInfo.PrimaryKey)
	query := fmt.Sprintf("SELECT %s FROM %s", pkColumns, s.quotedTableName(tableInfo.Name))

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

// syncMissingRows fetches and upserts rows that are missing in target
func (s *Syncer) syncMissingRows(ctx context.Context, tableInfo *table.Info, missingPKStrs []string, sourcePKs map[string][]any) error {
	if len(missingPKStrs) == 0 {
		return nil
	}

	// Process in batches
	batchSize := s.cfg.BatchSize
	for i := 0; i < len(missingPKStrs); i += batchSize {
		end := i + batchSize
		if end > len(missingPKStrs) {
			end = len(missingPKStrs)
		}
		batch := missingPKStrs[i:end]

		// Build PK values for this batch
		pkValues := make([][]any, len(batch))
		for j, pkStr := range batch {
			pkValues[j] = sourcePKs[pkStr]
		}

		// Fetch full rows from source
		rows, err := s.getRowsByPK(ctx, s.sourceDB, tableInfo, pkValues)
		if err != nil {
			return fmt.Errorf("failed to fetch source rows: %w", err)
		}

		// Upsert to target
		if len(rows) > 0 {
			if err := s.upsertData(ctx, tableInfo, rows); err != nil {
				return fmt.Errorf("failed to upsert rows: %w", err)
			}
		}

		s.logger.Debug("Reconciliation: synced batch",
			"table", tableInfo.Name,
			"batch", len(batch),
			"progress", fmt.Sprintf("%d/%d", end, len(missingPKStrs)),
		)
	}

	return nil
}

// deleteExtraRows deletes rows that exist in target but not in source
func (s *Syncer) deleteExtraRows(ctx context.Context, tableInfo *table.Info, extraPKStrs []string, targetPKs map[string][]any) error {
	if len(extraPKStrs) == 0 {
		return nil
	}

	// Build PK values for deletion
	pkValues := make([][]any, len(extraPKStrs))
	for i, pkStr := range extraPKStrs {
		pkValues[i] = targetPKs[pkStr]
	}

	// Delete in batches
	batchSize := s.cfg.BatchSize
	for i := 0; i < len(pkValues); i += batchSize {
		end := i + batchSize
		if end > len(pkValues) {
			end = len(pkValues)
		}
		batch := pkValues[i:end]

		if err := s.deleteRows(ctx, tableInfo, batch); err != nil {
			return fmt.Errorf("failed to delete rows: %w", err)
		}

		s.logger.Debug("Reconciliation: deleted batch",
			"table", tableInfo.Name,
			"batch", len(batch),
			"progress", fmt.Sprintf("%d/%d", end, len(pkValues)),
		)
	}

	return nil
}

// getRowsByPK fetches full rows by their primary key values
func (s *Syncer) getRowsByPK(ctx context.Context, db *sql.DB, tableInfo *table.Info, pkValues [][]any) ([][]any, error) {
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
			pkConds = append(pkConds, fmt.Sprintf("%s = $%d", s.quotedColumnName(col), argIdx))
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
