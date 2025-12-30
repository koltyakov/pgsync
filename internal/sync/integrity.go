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

const integrityFileName = "integrity.csv"

// writeIntegrityCSV writes integrity stats for each table into integrity.csv.
// This provides a comprehensive post-sync verification report comparing
// row counts, ID ranges, and timestamp ranges between source and target.
//
// Output file: integrity.csv in current working directory.
// If file exists, it will be overwritten.
func (s *Syncer) writeIntegrityCSV(ctx context.Context, tables []*table.Info) error {
	// Defensive: validate input
	if tables == nil {
		return fmt.Errorf("tables slice is nil")
	}

	// Create file in current working directory
	f, err := os.Create(integrityFileName)
	if err != nil {
		return fmt.Errorf("failed to create %s: %w", integrityFileName, err)
	}

	// Ensure file is closed and synced even on error
	var closeErr error
	defer func() {
		if syncErr := f.Sync(); syncErr != nil && closeErr == nil {
			closeErr = fmt.Errorf("failed to sync %s: %w", integrityFileName, syncErr)
		}
		if err := f.Close(); err != nil && closeErr == nil {
			closeErr = fmt.Errorf("failed to close %s: %w", integrityFileName, err)
		}
	}()

	w := csv.NewWriter(f)

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
		// Skip nil entries defensively
		if info == nil {
			s.logger.Warn("skipping nil table info in integrity check")
			continue
		}

		// Check for context cancellation
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("integrity check cancelled: %w", err)
		}

		// Row counts
		srcRows, err := s.getExactRowCount(ctx, s.sourceDB, info.Name)
		if err != nil {
			return fmt.Errorf("table %q: failed to get source row count: %w", info.Name, err)
		}
		tgtRows, err := s.getExactRowCount(ctx, s.targetDB, info.Name)
		if err != nil {
			return fmt.Errorf("table %q: failed to get target row count: %w", info.Name, err)
		}

		// ID column min/max (best-effort: single-column PK or column named 'id')
		var srcMinID, srcMaxID, tgtMinID, tgtMaxID string
		var idMinMatch, idMaxMatch string
		if idCol, ok := s.findIDColumn(info); ok {
			if srcMinID, srcMaxID, err = s.getMinMaxAsText(ctx, s.sourceDB, info.Name, idCol); err != nil {
				return fmt.Errorf("table %q: failed to get source id min/max: %w", info.Name, err)
			}
			if tgtMinID, tgtMaxID, err = s.getMinMaxAsText(ctx, s.targetDB, info.Name, idCol); err != nil {
				return fmt.Errorf("table %q: failed to get target id min/max: %w", info.Name, err)
			}
			idMinMatch = matchStatus(srcMinID, tgtMinID)
			idMaxMatch = matchStatus(srcMaxID, tgtMaxID)
		}

		// Timestamp min/max if column exists
		var srcMinTS, srcMaxTS, tgtMinTS, tgtMaxTS string
		var tsMinMatch, tsMaxMatch string
		if s.cfg.TimestampCol != "" && info.HasColumn(s.cfg.TimestampCol) {
			if srcMinTS, srcMaxTS, err = s.getMinMaxAsText(ctx, s.sourceDB, info.Name, s.cfg.TimestampCol); err != nil {
				return fmt.Errorf("table %q: failed to get source ts min/max: %w", info.Name, err)
			}
			if tgtMinTS, tgtMaxTS, err = s.getMinMaxAsText(ctx, s.targetDB, info.Name, s.cfg.TimestampCol); err != nil {
				return fmt.Errorf("table %q: failed to get target ts min/max: %w", info.Name, err)
			}
			tsMinMatch = matchStatus(srcMinTS, tgtMinTS)
			tsMaxMatch = matchStatus(srcMaxTS, tgtMaxTS)
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
			return fmt.Errorf("failed to write record for table %q: %w", info.Name, err)
		}
	}

	w.Flush()
	if err := w.Error(); err != nil {
		return fmt.Errorf("failed to flush csv writer: %w", err)
	}

	return closeErr
}

// matchStatus returns "Yes", "No", or empty string based on whether values match.
// Empty string is returned when both values are empty (no data to compare).
func matchStatus(a, b string) string {
	if a == "" && b == "" {
		return ""
	}
	if a == b {
		return "Yes"
	}
	return "No"
}

// getExactRowCount returns exact COUNT(*) from the given DB for a table.
// This is more accurate than pg_stat estimates but slower for large tables.
func (s *Syncer) getExactRowCount(ctx context.Context, dbh *sql.DB, tableName string) (int64, error) {
	if dbh == nil {
		return 0, fmt.Errorf("database connection is nil")
	}

	query := fmt.Sprintf("SELECT COUNT(*) FROM %s", s.quotedTableName(tableName))
	var cnt int64
	if err := dbh.QueryRowContext(ctx, query).Scan(&cnt); err != nil {
		return 0, fmt.Errorf("failed to count rows: %w", err)
	}
	return cnt, nil
}

// getMinMaxAsText returns MIN and MAX for a column, cast to text for CSV friendliness.
// Returns empty strings for NULL values (empty table or all-NULL column).
func (s *Syncer) getMinMaxAsText(ctx context.Context, dbh *sql.DB, tableName, column string) (string, string, error) {
	if dbh == nil {
		return "", "", fmt.Errorf("database connection is nil")
	}

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

// findIDColumn determines the best ID column to use for min/max analysis.
// Priority: single-column primary key > column named "id".
// Returns (column, true) if found, ("", false) otherwise.
func (s *Syncer) findIDColumn(info *table.Info) (string, bool) {
	if info == nil {
		return "", false
	}
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
