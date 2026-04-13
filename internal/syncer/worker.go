package syncer

import (
	"context"
	"fmt"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/koltyakov/pgsync/internal/db"
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

	// Notify progress handler of start
	s.progress.OnStart(tables)

	s.logger.Debug("Found tables to sync", "count", len(tables), "tables", strings.Join(tables, ", "))

	// Get FK dependencies for topological sort
	deps, err := s.inspector.GetTableDependencies(ctx)
	if err != nil {
		s.logger.Warn("Failed to get table dependencies, syncing without dependency order", "error", err)
		deps = nil
	}

	// Build a set of tables we're syncing (for filtering deps)
	tableSet := make(map[string]struct{})
	for _, t := range tables {
		tableSet[t] = struct{}{}
	}

	// Sort tables by dependency order (parents first, then children)
	sortedTables := topologicalSort(tables, deps, tableSet)
	s.logger.Debug("Tables sorted by dependency order", "order", strings.Join(sortedTables.Tables, " -> "))

	if len(sortedTables.CycleTables) > 0 {
		s.logger.Warn("Circular FK dependencies detected - tables appended in arbitrary order, FK violations may occur",
			"cycleTables", strings.Join(sortedTables.CycleTables, ", "))
	}

	// Get table metadata for all tables (in sorted order)
	// Also check target existence and filter columns
	tableInfos := make([]*table.Info, 0, len(sortedTables.Tables))
	for _, tableName := range sortedTables.Tables {
		// Check if table exists in target
		existsInTarget, err := s.inspector.TableExistsInTarget(ctx, tableName)
		if err != nil {
			s.logger.Warn("Failed to check target table existence", "table", tableName, "error", err)
		}
		if !existsInTarget {
			s.logger.Warn("Table missing in target database, skipping", "table", tableName)
			s.mu.Lock()
			s.skippedTables = append(s.skippedTables, tableName)
			s.mu.Unlock()
			continue
		}

		info, err := s.inspector.GetTableInfo(ctx, tableName)
		if err != nil {
			s.logger.Warn("Failed to get table info", "table", tableName, "error", err)
			continue
		}

		// Check which columns exist in target and filter
		targetCols, err := s.inspector.GetTargetColumns(ctx, tableName)
		if err != nil {
			s.logger.Warn("Failed to get target columns", "table", tableName, "error", err)
		} else if len(targetCols) > 0 {
			targetColSet := make(map[string]bool)
			for _, c := range targetCols {
				targetColSet[c] = true
			}
			// Find missing columns
			var missingCols []string
			originalColCount := len(info.Columns)
			for _, c := range info.Columns {
				if !targetColSet[c] {
					missingCols = append(missingCols, c)
				}
			}
			if len(missingCols) > 0 {
				s.logger.Warn("Columns missing in target, will be ignored",
					"table", tableName,
					"columns", strings.Join(missingCols, ", "))
				// Filter to only columns that exist in target
				info = info.FilterColumns(targetCols)
				s.logger.Info("Partial table sync",
					"table", tableName,
					"columns", fmt.Sprintf("%d/%d", len(info.Columns), originalColCount),
					"reason", "columns missing in target")
			}
		}

		tableInfos = append(tableInfos, info)
	}

	// Group tables into dependency levels for parallel processing within each level
	levels := groupByDependencyLevel(tableInfos, deps, tableSet)

	s.logger.Debug("Processing tables in dependency levels", "levels", len(levels))

	// Process each level - tables within a level can be processed in parallel
	for levelIdx, levelTables := range levels {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		// Sort tables within level by estimated work (largest first for better load balancing)
		sort.Slice(levelTables, func(i, j int) bool {
			return levelTables[i].EstimatedWork() > levelTables[j].EstimatedWork()
		})

		levelNames := make([]string, len(levelTables))
		for i, t := range levelTables {
			levelNames[i] = t.Name
		}
		s.logger.Debug("Processing dependency level", "level", levelIdx+1, "tables", strings.Join(levelNames, ", "))

		// Process this level with worker pool
		if err := s.syncLevel(ctx, levelTables); err != nil {
			return fmt.Errorf("failed to sync level %d: %w", levelIdx+1, err)
		}
	}

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

	// Notify progress handler of completion
	s.progress.OnComplete(totalUpserts, totalDeletes)

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
			s.logger.Warn("Failed to write integrity report", "error", err)
		} else {
			s.logger.Info("Integrity report written", "path", "integrity.csv")
		}
	}
	return nil
}

// syncLevel processes all tables in a dependency level using a worker pool
func (s *Syncer) syncLevel(ctx context.Context, tableInfos []*table.Info) error {
	if len(tableInfos) == 0 {
		return nil
	}

	// Use fewer workers if we have fewer tables than parallel setting
	numWorkers := s.cfg.Parallel
	if len(tableInfos) < numWorkers {
		numWorkers = len(tableInfos)
	}

	workChan := make(chan tableWork, len(tableInfos))
	errChan := make(chan error, len(tableInfos))
	var wg sync.WaitGroup

	// Start worker goroutines
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			s.levelWorkerWithIndex(ctx, workerID, workChan, errChan)
		}(i)
	}

	// Send work to workers with index for progress tracking
	for i, info := range tableInfos {
		select {
		case workChan <- tableWork{info: info, index: i}:
		case <-ctx.Done():
			close(workChan)
			wg.Wait()
			return ctx.Err()
		}
	}
	close(workChan)

	// Wait for all workers to complete
	wg.Wait()
	close(errChan)

	// Collect any errors
	var errs []string
	for err := range errChan {
		errs = append(errs, err.Error())
	}

	if len(errs) > 0 {
		return fmt.Errorf("errors syncing tables: %s", strings.Join(errs, "; "))
	}

	return nil
}

// tableWork pairs table info with its index for progress reporting
type tableWork struct {
	info  *table.Info
	index int
}

// levelWorkerWithIndex processes table sync jobs with progress reporting
func (s *Syncer) levelWorkerWithIndex(ctx context.Context, _ int, workChan <-chan tableWork, errChan chan<- error) {
	for {
		select {
		case work, ok := <-workChan:
			if !ok {
				return // Channel closed
			}
			// Filter columns if specified in config
			tableInfo := work.info
			originalColumns := work.info.Columns
			originalColCount := len(originalColumns)

			if cols, ok := s.cfg.IncludeColumns[work.info.Name]; ok && len(cols) > 0 {
				tableInfo = work.info.FilterColumns(cols)
				if len(tableInfo.Columns) < originalColCount {
					ignoredCols := findIgnoredColumns(originalColumns, tableInfo.Columns)
					s.logger.Info("Partial table sync (columns filtered by config)",
						"table", work.info.Name,
						"columns", fmt.Sprintf("%d/%d", len(tableInfo.Columns), originalColCount),
						"syncing", strings.Join(tableInfo.Columns, ", "))
					s.progress.OnPartialSync(work.info.Name, tableInfo.Columns, ignoredCols, "columns deselected")
				}
			} else if len(tableInfo.Columns) < originalColCount {
				// Columns were filtered due to missing in target (done earlier in Sync)
				ignoredCols := findIgnoredColumns(originalColumns, tableInfo.Columns)
				s.logger.Info("Partial table sync (columns missing in target)",
					"table", work.info.Name,
					"columns", fmt.Sprintf("%d/%d", len(tableInfo.Columns), originalColCount))
				s.progress.OnPartialSync(work.info.Name, tableInfo.Columns, ignoredCols, "columns missing in target")
			}

			// Notify progress handler of table start
			s.progress.OnTableStart(tableInfo.Name, work.index)

			if err := s.syncTable(ctx, tableInfo); err != nil {
				s.logger.Error("Error syncing table", "table", tableInfo.Name, "error", err)
				errChan <- fmt.Errorf("table %s: %w", tableInfo.Name, err)
			}
		case <-ctx.Done():
			return // Context cancelled
		}
	}
}

// findIgnoredColumns returns columns from original that are not in syncing
func findIgnoredColumns(original, syncing []string) []string {
	syncingSet := make(map[string]struct{}, len(syncing))
	for _, c := range syncing {
		syncingSet[c] = struct{}{}
	}
	var ignored []string
	for _, c := range original {
		if _, exists := syncingSet[c]; !exists {
			ignored = append(ignored, c)
		}
	}
	return ignored
}

type topoSortResult struct {
	Tables      []string
	CycleTables []string
}

// topologicalSort sorts tables so that parent tables come before children (FK dependencies)
func topologicalSort(tables []string, deps []db.TableDependency, tableSet map[string]struct{}) topoSortResult {
	if len(deps) == 0 {
		return topoSortResult{Tables: tables}
	}

	// Build adjacency list (table -> tables it depends on)
	dependsOn := make(map[string][]string)
	for _, dep := range deps {
		// Only consider dependencies where both tables are in our sync set
		_, tableInSet := tableSet[dep.Table]
		_, dependsOnInSet := tableSet[dep.DependsOn]
		if tableInSet && dependsOnInSet {
			dependsOn[dep.Table] = append(dependsOn[dep.Table], dep.DependsOn)
		}
	}

	// Kahn's algorithm for topological sort
	// Calculate in-degree (number of dependencies) for each table
	inDegree := make(map[string]int)
	for _, t := range tables {
		inDegree[t] = 0
	}
	for t, deps := range dependsOn {
		inDegree[t] = len(deps)
	}

	// Start with tables that have no dependencies
	var queue []string
	for _, t := range tables {
		if inDegree[t] == 0 {
			queue = append(queue, t)
		}
	}

	// Build reverse adjacency list (table -> tables that depend on it)
	dependedBy := make(map[string][]string)
	for t, deps := range dependsOn {
		for _, dep := range deps {
			dependedBy[dep] = append(dependedBy[dep], t)
		}
	}

	// Process queue
	var sorted []string
	for len(queue) > 0 {
		// Sort queue for deterministic order
		sort.Strings(queue)
		t := queue[0]
		queue = queue[1:]
		sorted = append(sorted, t)

		// Reduce in-degree for tables that depend on this one
		for _, dependent := range dependedBy[t] {
			inDegree[dependent]--
			if inDegree[dependent] == 0 {
				queue = append(queue, dependent)
			}
		}
	}

	var cycleTables []string
	if len(sorted) < len(tables) {
		sortedSet := make(map[string]bool)
		for _, t := range sorted {
			sortedSet[t] = true
		}
		for _, t := range tables {
			if !sortedSet[t] {
				sorted = append(sorted, t)
				cycleTables = append(cycleTables, t)
			}
		}
	}

	return topoSortResult{Tables: sorted, CycleTables: cycleTables}
}

// groupByDependencyLevel groups tables into levels where tables in the same level
// have no dependencies on each other and can be synced in parallel
func groupByDependencyLevel(tableInfos []*table.Info, deps []db.TableDependency, tableSet map[string]struct{}) [][]*table.Info {
	if len(deps) == 0 || len(tableInfos) == 0 {
		// No dependencies - all tables can be synced in parallel
		return [][]*table.Info{tableInfos}
	}

	// Build dependency map
	dependsOn := make(map[string]map[string]struct{})
	for _, dep := range deps {
		_, tableInSet := tableSet[dep.Table]
		_, dependsOnInSet := tableSet[dep.DependsOn]
		if tableInSet && dependsOnInSet {
			if dependsOn[dep.Table] == nil {
				dependsOn[dep.Table] = make(map[string]struct{})
			}
			dependsOn[dep.Table][dep.DependsOn] = struct{}{}
		}
	}

	// Build table info map for lookup
	infoMap := make(map[string]*table.Info)
	for _, info := range tableInfos {
		infoMap[info.Name] = info
	}

	// Assign levels - a table's level is 1 + max level of its dependencies
	levels := make(map[string]int)
	var assignLevel func(tableName string) int
	assignLevel = func(tableName string) int {
		if level, ok := levels[tableName]; ok {
			return level
		}

		maxDepLevel := -1
		for dep := range dependsOn[tableName] {
			if _, exists := tableSet[dep]; exists {
				depLevel := assignLevel(dep)
				if depLevel > maxDepLevel {
					maxDepLevel = depLevel
				}
			}
		}

		levels[tableName] = maxDepLevel + 1
		return levels[tableName]
	}

	// Calculate level for each table
	for _, info := range tableInfos {
		assignLevel(info.Name)
	}

	// Group by level
	maxLevel := 0
	for _, level := range levels {
		if level > maxLevel {
			maxLevel = level
		}
	}

	result := make([][]*table.Info, maxLevel+1)
	for _, info := range tableInfos {
		level := levels[info.Name]
		result[level] = append(result[level], info)
	}

	// Remove empty levels
	var filtered [][]*table.Info
	for _, level := range result {
		if len(level) > 0 {
			filtered = append(filtered, level)
		}
	}

	return filtered
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
		parts = append(parts, strconv.FormatInt(h, 10)+"h")
	}
	if m > 0 {
		parts = append(parts, strconv.FormatInt(m, 10)+"m")
	}
	if h > 0 || m > 0 || sec > 0 {
		parts = append(parts, strconv.FormatInt(sec, 10)+"s")
	}
	if ms > 0 {
		parts = append(parts, strconv.FormatInt(ms, 10)+"ms")
	}
	if len(parts) == 0 {
		return "0ms"
	}
	return strings.Join(parts, "")
}
