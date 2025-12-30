package sync

import (
	"context"
	"database/sql"
	"encoding/csv"
	"fmt"
	"os"
	"strings"

	"github.com/koltyakov/pgsync/internal/table"
)

// writeIntegrityCSV writes integrity stats for each table into integrity.csv
func (s *Syncer) writeIntegrityCSV(ctx context.Context, tables []*table.Info) error {
	// Create file in current working directory
	f, err := os.Create("integrity.csv")
	if err != nil {
		return fmt.Errorf("failed to create integrity.csv: %w", err)
	}
	defer func() { _ = f.Close() }()

	w := csv.NewWriter(f)
	defer w.Flush()

	// Header
	header := []string{
		"Table Name",
		"Source Rows", "Target Rows", "Rows match",
		"Source min(id)", "Target min(id)", "min(id) match",
		"Source max(id)", "Target max(id)", "max(id) match",
		"Source min(ts)", "Target min(ts)", "min(ts) match",
		"Source max(ts)", "Target max(ts)", "max(ts) match",
	}
	if err := w.Write(header); err != nil {
		return fmt.Errorf("failed to write header: %w", err)
	}

	for _, info := range tables {
		// Check for context cancellation
		if ctx.Err() != nil {
			return ctx.Err()
		}

		// Row counts
		srcRows, err := s.getExactRowCount(ctx, s.sourceDB, info.Name)
		if err != nil {
			return fmt.Errorf("%s: failed to get source row count: %w", info.Name, err)
		}
		tgtRows, err := s.getExactRowCount(ctx, s.targetDB, info.Name)
		if err != nil {
			return fmt.Errorf("%s: failed to get target row count: %w", info.Name, err)
		}

		// ID column min/max (best-effort: single-column PK or column named 'id')
		var srcMinID, srcMaxID, tgtMinID, tgtMaxID string
		var idMinMatch, idMaxMatch string
		if idCol, ok := s.findIDColumn(info); ok {
			if srcMinID, srcMaxID, err = s.getMinMaxAsText(ctx, s.sourceDB, info.Name, idCol); err != nil {
				return fmt.Errorf("%s: failed to get source id min/max: %w", info.Name, err)
			}
			if tgtMinID, tgtMaxID, err = s.getMinMaxAsText(ctx, s.targetDB, info.Name, idCol); err != nil {
				return fmt.Errorf("%s: failed to get target id min/max: %w", info.Name, err)
			}
			if srcMinID != "" || tgtMinID != "" {
				if srcMinID == tgtMinID {
					idMinMatch = "Yes"
				} else {
					idMinMatch = "No"
				}
			}
			if srcMaxID != "" || tgtMaxID != "" {
				if srcMaxID == tgtMaxID {
					idMaxMatch = "Yes"
				} else {
					idMaxMatch = "No"
				}
			}
		}

		// Timestamp min/max if column exists
		var srcMinTS, srcMaxTS, tgtMinTS, tgtMaxTS string
		var tsMinMatch, tsMaxMatch string
		if info.HasColumn(s.cfg.TimestampCol) && s.cfg.TimestampCol != "" {
			if srcMinTS, srcMaxTS, err = s.getMinMaxAsText(ctx, s.sourceDB, info.Name, s.cfg.TimestampCol); err != nil {
				return fmt.Errorf("%s: failed to get source ts min/max: %w", info.Name, err)
			}
			if tgtMinTS, tgtMaxTS, err = s.getMinMaxAsText(ctx, s.targetDB, info.Name, s.cfg.TimestampCol); err != nil {
				return fmt.Errorf("%s: failed to get target ts min/max: %w", info.Name, err)
			}
			if srcMinTS != "" || tgtMinTS != "" {
				if srcMinTS == tgtMinTS {
					tsMinMatch = "Yes"
				} else {
					tsMinMatch = "No"
				}
			}
			if srcMaxTS != "" || tgtMaxTS != "" {
				if srcMaxTS == tgtMaxTS {
					tsMaxMatch = "Yes"
				} else {
					tsMaxMatch = "No"
				}
			}
		}

		rowsMatch := "No"
		if srcRows == tgtRows {
			rowsMatch = "Yes"
		}

		rec := []string{
			info.Name,
			fmt.Sprintf("%d", srcRows), fmt.Sprintf("%d", tgtRows), rowsMatch,
			srcMinID, tgtMinID, idMinMatch,
			srcMaxID, tgtMaxID, idMaxMatch,
			srcMinTS, tgtMinTS, tsMinMatch,
			srcMaxTS, tgtMaxTS, tsMaxMatch,
		}
		if err := w.Write(rec); err != nil {
			return fmt.Errorf("failed to write record: %w", err)
		}
	}

	w.Flush()
	if err := w.Error(); err != nil {
		return fmt.Errorf("failed to flush csv writer: %w", err)
	}
	return nil
}

// getExactRowCount returns exact COUNT(*) from the given DB for a table
func (s *Syncer) getExactRowCount(ctx context.Context, dbh *sql.DB, tableName string) (int64, error) {
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s", s.quotedTableName(tableName))
	var cnt int64
	if err := dbh.QueryRowContext(ctx, query).Scan(&cnt); err != nil {
		return 0, fmt.Errorf("failed to count rows: %w", err)
	}
	return cnt, nil
}

// getMinMaxAsText returns MIN and MAX for a column, cast to text for CSV friendliness
func (s *Syncer) getMinMaxAsText(ctx context.Context, dbh *sql.DB, tableName, column string) (string, string, error) {
	query := fmt.Sprintf("SELECT MIN((%s)::text), MAX((%s)::text) FROM %s",
		s.quotedColumnName(column), s.quotedColumnName(column), s.quotedTableName(tableName))
	var minVal, maxVal sql.NullString
	if err := dbh.QueryRowContext(ctx, query).Scan(&minVal, &maxVal); err != nil {
		return "", "", fmt.Errorf("failed to get min/max: %w", err)
	}
	var minStr, maxStr string
	if minVal.Valid {
		minStr = minVal.String
	}
	if maxVal.Valid {
		maxStr = maxVal.String
	}
	return minStr, maxStr, nil
}

// findIDColumn determines the best ID column to use for min/max
func (s *Syncer) findIDColumn(info *table.Info) (string, bool) {
	if len(info.PrimaryKey) == 1 {
		return info.PrimaryKey[0], true
	}
	for _, c := range info.Columns {
		if strings.EqualFold(c, "id") {
			return c, true
		}
	}
	return "", false
}
