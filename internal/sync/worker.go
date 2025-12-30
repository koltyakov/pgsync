package sync

import (
	"context"
	"log/slog"
	"path/filepath"
	"sort"
	"strings"
	gosync "sync"
	"time"

	"github.com/koltyakov/pgsync/internal/table"
)

// Sync performs the synchronization process
func (s *Syncer) Sync(ctx context.Context) error {
	start := time.Now()

	// Get list of tables to sync
	tables, err := s.getTablesList(ctx)
	if err != nil {
		return err
	}

	if len(tables) == 0 {
		s.logger.Info("No tables to sync")
		return nil
	}

	s.logger.Debug("Found tables to sync", "count", len(tables), "tables", strings.Join(tables, ", "))

	// Get table metadata for all tables
	tableInfos := make([]*table.Info, 0, len(tables))
	for _, tableName := range tables {
		info, err := s.inspector.GetTableInfo(ctx, tableName)
		if err != nil {
			s.logger.Warn("Failed to get table info", "table", tableName, "error", err)
			continue
		}
		tableInfos = append(tableInfos, info)
	}

	// Sort tables by estimated work (row count * complexity)
	sort.Slice(tableInfos, func(i, j int) bool {
		return tableInfos[i].EstimatedWork() > tableInfos[j].EstimatedWork()
	})

	// Create work channel and worker pool
	workChan := make(chan *table.Info, len(tableInfos))
	var wg gosync.WaitGroup

	// Start worker goroutines
	for i := 0; i < s.cfg.Parallel; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			s.worker(ctx, workerID, workChan)
		}(i)
	}

	// Send work to workers
	for _, info := range tableInfos {
		select {
		case workChan <- info:
		case <-ctx.Done():
			close(workChan)
			wg.Wait()
			return ctx.Err()
		}
	}
	close(workChan)

	// Wait for all workers to complete
	wg.Wait()

	// Check if context was cancelled
	if ctx.Err() != nil {
		return ctx.Err()
	}

	// Log totals
	s.mu.Lock()
	totalUpserts := s.totalUpserts
	totalDeletes := s.totalDeletes
	s.mu.Unlock()

	elapsed := time.Since(start)
	s.logger.Info("All table syncs completed",
		"duration", formatHumanDuration(elapsed),
		"synced", totalUpserts,
		"deleted", totalDeletes,
	)

	// Log skipped tables if any
	s.mu.Lock()
	skipped := append([]string(nil), s.skippedTables...)
	s.mu.Unlock()
	if len(skipped) > 0 {
		s.logger.Warn("Tables skipped", "count", len(skipped), "tables", strings.Join(skipped, ", "))
	}

	// Post-sync integrity check and CSV export (optional)
	if s.cfg.Integrity {
		if err := s.writeIntegrityCSV(ctx, tableInfos); err != nil {
			s.logger.Warn("Failed to write integrity.csv", "error", err)
		} else {
			s.logger.Info("Integrity report written to integrity.csv")
		}
	}
	return nil
}

// worker processes table sync jobs
func (s *Syncer) worker(ctx context.Context, workerID int, workChan <-chan *table.Info) {
	for {
		select {
		case tableInfo, ok := <-workChan:
			if !ok {
				return // Channel closed
			}
			if err := s.syncTable(ctx, tableInfo); err != nil {
				s.logger.Error("Error syncing table", "table", tableInfo.Name, "error", err)
			}
		case <-ctx.Done():
			return // Context cancelled
		}
	}
}

// getTablesList returns the list of tables to sync based on include/exclude filters
func (s *Syncer) getTablesList(ctx context.Context) ([]string, error) {
	allTables, err := s.inspector.GetTables(ctx)
	if err != nil {
		return nil, err
	}

	if len(s.cfg.IncludeTables) > 0 {
		// Filter to only included tables (supports wildcards)
		var filtered []string
		for _, table := range allTables {
			if s.matchesPattern(table, s.cfg.IncludeTables) {
				filtered = append(filtered, table)
			}
		}
		return filtered, nil
	}

	if len(s.cfg.ExcludeTables) > 0 {
		// Filter out excluded tables (supports wildcards)
		var filtered []string
		for _, table := range allTables {
			if !s.matchesPattern(table, s.cfg.ExcludeTables) {
				filtered = append(filtered, table)
			}
		}
		return filtered, nil
	}

	return allTables, nil
}

// matchesPattern checks if a table name matches any of the given patterns (supports wildcards)
func (s *Syncer) matchesPattern(tableName string, patterns []string) bool {
	for _, pattern := range patterns {
		pattern = strings.TrimSpace(pattern)
		// Try direct match first (for backward compatibility)
		if pattern == tableName {
			return true
		}
		// Try wildcard match
		if matched, err := filepath.Match(pattern, tableName); err == nil && matched {
			return true
		}
	}
	return false
}

// formatHumanDuration renders duration as [Hh][Mm][Ss][ms], with milliseconds precision
func formatHumanDuration(d time.Duration) string {
	if d < 0 {
		d = -d
	}
	msTotal := d.Milliseconds()
	if msTotal == 0 {
		return "0ms"
	}
	const (
		msPerSecond = int64(1000)
		msPerMinute = msPerSecond * 60
		msPerHour   = msPerMinute * 60
	)
	h := msTotal / msPerHour
	msTotal %= msPerHour
	m := msTotal / msPerMinute
	msTotal %= msPerMinute
	sec := msTotal / msPerSecond
	ms := msTotal % msPerSecond

	parts := make([]string, 0, 4)
	if h > 0 {
		parts = append(parts, slog.Int64("h", h).Value.String()+"h")
	}
	if m > 0 {
		parts = append(parts, slog.Int64("m", m).Value.String()+"m")
	}
	if h > 0 || m > 0 || sec > 0 {
		parts = append(parts, slog.Int64("s", sec).Value.String()+"s")
	}
	if ms > 0 {
		parts = append(parts, slog.Int64("ms", ms).Value.String()+"ms")
	}
	if len(parts) == 0 {
		return "0ms"
	}
	return strings.Join(parts, "")
}
