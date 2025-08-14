package sync

import (
	"context"
	"database/sql"
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/koltyakov/pgsync/internal/config"
	"github.com/koltyakov/pgsync/internal/db"
	"github.com/koltyakov/pgsync/internal/table"
	_ "github.com/lib/pq"
)

// Syncer handles the synchronization between two PostgreSQL databases
type Syncer struct {
	cfg            *config.Config
	sourceDB       *sql.DB
	targetDB       *sql.DB
	inspector      *db.Inspector
	mu             sync.Mutex
	upsertsByTable map[string]int64
	deletesByTable map[string]int64
	totalUpserts   int64
	totalDeletes   int64
	skippedTables  []string
}

// quotedTableName returns a properly quoted table name for PostgreSQL
// This handles CamelCase table names created by .NET Entity Framework
func (s *Syncer) quotedTableName(tableName string) string {
	return fmt.Sprintf("%s.\"%s\"", s.cfg.Schema, tableName)
}

// quotedColumnName returns a properly quoted column name for PostgreSQL
// This handles CamelCase column names created by .NET Entity Framework
func (s *Syncer) quotedColumnName(columnName string) string {
	return fmt.Sprintf("\"%s\"", columnName)
}

// quotedColumnsList returns a comma-separated list of quoted column names
func (s *Syncer) quotedColumnsList(columns []string) string {
	quoted := make([]string, len(columns))
	for i, col := range columns {
		quoted[i] = s.quotedColumnName(col)
	}
	return strings.Join(quoted, ", ")
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

// New creates a new Syncer instance
func New(cfg *config.Config) (*Syncer, error) {
	sourceDB, err := sql.Open("postgres", cfg.SourceDB)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to source database: %w", err)
	}

	targetDB, err := sql.Open("postgres", cfg.TargetDB)
	if err != nil {
		_ = sourceDB.Close() // Ignore close error when handling connection error
		return nil, fmt.Errorf("failed to connect to target database: %w", err)
	}

	// Test connections
	if err := sourceDB.Ping(); err != nil {
		_ = sourceDB.Close() // Ignore close error when handling ping error
		_ = targetDB.Close() // Ignore close error when handling ping error
		return nil, fmt.Errorf("failed to ping source database: %w", err)
	}

	if err := targetDB.Ping(); err != nil {
		_ = sourceDB.Close() // Ignore close error when handling ping error
		_ = targetDB.Close() // Ignore close error when handling ping error
		return nil, fmt.Errorf("failed to ping target database: %w", err)
	}

	inspector := db.NewInspector(sourceDB, targetDB, cfg.Schema)

	return &Syncer{
		cfg:            cfg,
		sourceDB:       sourceDB,
		targetDB:       targetDB,
		inspector:      inspector,
		upsertsByTable: make(map[string]int64),
		deletesByTable: make(map[string]int64),
		skippedTables:  make([]string, 0),
	}, nil
}

// Close closes all database connections
func (s *Syncer) Close() error {
	var errs []string

	if err := s.sourceDB.Close(); err != nil {
		errs = append(errs, fmt.Sprintf("source DB: %v", err))
	}
	if err := s.targetDB.Close(); err != nil {
		errs = append(errs, fmt.Sprintf("target DB: %v", err))
	}

	if len(errs) > 0 {
		return fmt.Errorf("errors closing databases: %s", strings.Join(errs, ", "))
	}
	return nil
}

// Sync performs the synchronization process
func (s *Syncer) Sync() error {
	ctx := context.Background()
	start := time.Now()

	// Get list of tables to sync
	tables, err := s.getTablesList(ctx)
	if err != nil {
		return fmt.Errorf("failed to get tables list: %w", err)
	}

	if len(tables) == 0 {
		log.Println("No tables to sync")
		return nil
	}

	if s.cfg.Verbose {
		log.Printf("Found %d tables to sync: %s", len(tables), strings.Join(tables, ", "))
	}

	// Get table metadata for all tables
	tableInfos := make([]*table.Info, 0, len(tables))
	for _, tableName := range tables {
		info, err := s.inspector.GetTableInfo(ctx, tableName)
		if err != nil {
			log.Printf("%s: warning - failed to get table info: %v", tableName, err)
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
	var wg sync.WaitGroup

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
		workChan <- info
	}
	close(workChan)

	// Wait for all workers to complete
	wg.Wait()

	// Log totals
	s.mu.Lock()
	totalUpserts := s.totalUpserts
	totalDeletes := s.totalDeletes
	s.mu.Unlock()

	elapsed := time.Since(start)
	log.Printf("All table syncs completed in %s. Totals: synced %d rows, deleted %d rows", formatHumanDuration(elapsed), totalUpserts, totalDeletes)

	// Log skipped tables if any
	s.mu.Lock()
	skipped := append([]string(nil), s.skippedTables...)
	s.mu.Unlock()
	if len(skipped) > 0 {
		log.Printf("WARNING Tables skipped (%d): %s", len(skipped), strings.Join(skipped, ", "))
	}

	// Post-sync integrity check and CSV export
	if err := s.writeIntegrityCSV(ctx, tableInfos); err != nil {
		log.Printf("WARNING failed to write integrity.csv: %v", err)
	} else {
		log.Printf("Integrity report written to integrity.csv")
	}
	return nil
}

// formatHumanDuration renders duration as [Hh][Mm][Ss][ms], with milliseconds precision (e.g., 2m21s883ms)
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
	s := msTotal / msPerSecond
	ms := msTotal % msPerSecond

	parts := make([]string, 0, 4)
	if h > 0 {
		parts = append(parts, fmt.Sprintf("%dh", h))
	}
	if m > 0 {
		parts = append(parts, fmt.Sprintf("%dm", m))
	}
	if h > 0 || m > 0 || s > 0 {
		if h > 0 || m > 0 {
			parts = append(parts, fmt.Sprintf("%ds", s))
		} else if s > 0 {
			parts = append(parts, fmt.Sprintf("%ds", s))
		}
	}
	if ms > 0 {
		parts = append(parts, fmt.Sprintf("%dms", ms))
	}
	if len(parts) == 0 {
		return "0ms"
	}
	return strings.Join(parts, "")
}

// worker processes table sync jobs
func (s *Syncer) worker(ctx context.Context, _ int /* worker id*/, workChan <-chan *table.Info) {
	for tableInfo := range workChan {
		if err := s.syncTable(ctx, tableInfo); err != nil {
			log.Printf("%s: error syncing - %v", tableInfo.Name, err)
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

// syncTable synchronizes a single table
func (s *Syncer) syncTable(ctx context.Context, tableInfo *table.Info) error {
	tableName := tableInfo.Name

	// Capture baseline counts and always log per-table delta at exit
	preUp, preDel := s.tableCounts(tableName)
	defer func() {
		postUp, postDel := s.tableCounts(tableName)
		log.Printf("%s: synced %d rows, deleted %d rows", tableName, postUp-preUp, postDel-preDel)
	}()

	// Check if table has timestamp column
	hasTimestamp := tableInfo.HasColumn(s.cfg.TimestampCol)

	if !hasTimestamp {
		// For small tables (<1000 rows) with primary key, perform full upsert + deletion check
		if len(tableInfo.PrimaryKey) == 0 {
			log.Printf("%s: has no %s column and no primary key, skipping", tableName, s.cfg.TimestampCol)
			return nil
		}
		if tableInfo.RowCount <= 1000 {
			if s.cfg.Verbose {
				log.Printf("%s: no %s column, small table (%d rows) — checking diff", tableName, s.cfg.TimestampCol, tableInfo.RowCount)
			}
			return s.syncTableFullSmall(ctx, tableInfo)
		}
		log.Printf("%s: has no %s column and is large (~%d rows), skipping full sync", tableName, s.cfg.TimestampCol, tableInfo.RowCount)
		s.addSkipped(tableName)
		return nil
	}

	// Always prefer timestamp from target DB for determining sync point
	targetMaxTimestamp, err := s.getMaxTimestampFromTarget(ctx, tableName)
	if err != nil {
		return fmt.Errorf("failed to get max timestamp from target: %w", err)
	}

	var lastSync time.Time
	var timestampSource string
	if !targetMaxTimestamp.IsZero() {
		// Use target DB timestamp as the starting point
		lastSync = targetMaxTimestamp
		timestampSource = "target DB"
	} else {
		// If target DB has no data, start from zero time
		lastSync = time.Time{}
		timestampSource = "zero time (empty target)"
	}
	return s.syncTableIncremental(ctx, tableInfo, lastSync, timestampSource)
}

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
		// Derive PK key
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

	// Apply changes
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
	if s.cfg.Verbose {
		if len(toUpsert) == 0 && len(toDelete) == 0 {
			log.Printf("%s: small-table diff — no changes", tableInfo.Name)
		} else {
			log.Printf("%s: small-table diff — changed %d, deleted %d", tableInfo.Name, len(toUpsert), len(toDelete))
		}
	}

	return nil
}

// getAllSourceData retrieves all rows for a table (no timestamp filter)
func (s *Syncer) getAllSourceData(ctx context.Context, tableInfo *table.Info) ([][]any, error) {
	columns := s.quotedColumnsList(tableInfo.Columns)
	query := fmt.Sprintf("SELECT %s FROM %s", columns, s.quotedTableName(tableInfo.Name))

	rows, err := s.sourceDB.QueryContext(ctx, query)
	if err != nil {
		return nil, err
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
			return nil, err
		}
		result = append(result, values)
	}
	return result, nil
}

// getAllTargetData retrieves all rows from the target table
func (s *Syncer) getAllTargetData(ctx context.Context, tableInfo *table.Info) ([][]any, error) {
	columns := s.quotedColumnsList(tableInfo.Columns)
	query := fmt.Sprintf("SELECT %s FROM %s", columns, s.quotedTableName(tableInfo.Name))

	rows, err := s.targetDB.QueryContext(ctx, query)
	if err != nil {
		return nil, err
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
			return nil, err
		}
		result = append(result, values)
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

// getMaxTimestampFromTarget gets the maximum timestamp from target table
func (s *Syncer) getMaxTimestampFromTarget(ctx context.Context, tableName string) (time.Time, error) {
	query := fmt.Sprintf(`
		SELECT COALESCE(MAX(%s), '1970-01-01'::timestamp) 
		FROM %s`,
		s.quotedColumnName(s.cfg.TimestampCol), s.quotedTableName(tableName))

	var maxTS time.Time
	err := s.targetDB.QueryRowContext(ctx, query).Scan(&maxTS)
	if err != nil {
		// If table doesn't exist in target, return zero time
		if strings.Contains(err.Error(), "does not exist") || strings.Contains(err.Error(), "relation") {
			return time.Time{}, nil
		}
		return time.Time{}, err
	}

	// If the result is the epoch time (1970-01-01), treat it as zero
	epoch := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
	if maxTS.Equal(epoch) {
		return time.Time{}, nil
	}

	return maxTS, nil
}

// getSourceMaxTimestamp gets the maximum timestamp from source table
func (s *Syncer) getSourceMaxTimestamp(ctx context.Context, tableName string) (time.Time, error) {
	query := fmt.Sprintf(`
		SELECT COALESCE(MAX(%s), '1970-01-01'::timestamp) 
		FROM %s`,
		s.quotedColumnName(s.cfg.TimestampCol), s.quotedTableName(tableName))

	var maxTS time.Time
	err := s.sourceDB.QueryRowContext(ctx, query).Scan(&maxTS)
	if err != nil {
		return time.Time{}, err
	}

	// If the result is the epoch time (1970-01-01), treat it as zero
	epoch := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
	if maxTS.Equal(epoch) {
		return time.Time{}, nil
	}

	return maxTS, nil
}

// syncTableIncremental performs incremental sync based on timestamp
func (s *Syncer) syncTableIncremental(ctx context.Context, tableInfo *table.Info, lastSync time.Time, timestampSource string) error {
	tableName := tableInfo.Name

	// Get the range of timestamps to process
	minTS, maxTS, err := s.getTimestampRange(ctx, tableName, lastSync)
	if err != nil {
		return fmt.Errorf("failed to get timestamp range: %w", err)
	}

	if minTS.IsZero() {
		if s.cfg.Verbose {
			log.Printf("%s: no new data found", tableName)
		}

		// Even when there's no new data, check if we should handle deletions
		// Get source max timestamp to compare with target
		sourceMaxTS, err := s.getSourceMaxTimestamp(ctx, tableName)
		if err != nil {
			return fmt.Errorf("failed to get source max timestamp: %w", err)
		}

		// Get target max timestamp
		targetMaxTS, err := s.getMaxTimestampFromTarget(ctx, tableName)
		if err != nil {
			return fmt.Errorf("failed to get target max timestamp: %w", err)
		}

		// Special case: if source is empty and target has rows, delete all in target in chunks
		sourceHas, err := s.tableHasRows(ctx, s.sourceDB, tableName)
		if err != nil {
			return fmt.Errorf("failed to check source rows: %w", err)
		}
		targetHas, err := s.tableHasRows(ctx, s.targetDB, tableName)
		if err != nil {
			return fmt.Errorf("failed to check target rows: %w", err)
		}
		if !sourceHas && targetHas {
			if s.cfg.Verbose {
				log.Printf("%s: source is empty, deleting all rows from target (chunked)", tableName)
			}
			if err := s.deleteAllInChunks(ctx, tableInfo); err != nil {
				return fmt.Errorf("failed to delete all target rows: %w", err)
			}
			return nil
		}

		// Only handle deletions if target is caught up with source
		if !targetMaxTS.IsZero() && !sourceMaxTS.IsZero() && (targetMaxTS.Equal(sourceMaxTS) || targetMaxTS.After(sourceMaxTS)) {
			if s.cfg.Verbose {
				log.Printf("%s: checking for deletions", tableName)
			}
			if err := s.handleDeletedRows(ctx, tableInfo); err != nil {
				return fmt.Errorf("failed to handle deleted rows: %w", err)
			}
		}

		return nil
	}

	if s.cfg.Verbose {
		log.Printf("%s: using last sync timestamp from %s (%s)", tableName, timestampSource, lastSync.Format(time.RFC3339))
		log.Printf("%s: syncing from %s", tableName, minTS.Format(time.RFC3339))
	}

	// Process data in batches
	currentTS := minTS
	for currentTS.Before(maxTS) || currentTS.Equal(maxTS) {
		nextTS, err := s.processBatch(ctx, tableInfo, currentTS, maxTS)
		if err != nil {
			return fmt.Errorf("failed to process batch: %w", err)
		}

		currentTS = nextTS.Add(time.Microsecond) // Move slightly forward to avoid duplicate processing
	}

	// After successful incremental sync, check if we should handle deletions
	// Get source max timestamp to compare with target
	sourceMaxTS, err := s.getSourceMaxTimestamp(ctx, tableName)
	if err != nil {
		return fmt.Errorf("failed to get source max timestamp: %w", err)
	}

	// Get updated target max timestamp (should now include the data we just synced)
	targetMaxTS, err := s.getMaxTimestampFromTarget(ctx, tableName)
	if err != nil {
		return fmt.Errorf("failed to get target max timestamp: %w", err)
	}

	// Only handle deletions if target is caught up with source
	if !targetMaxTS.IsZero() && !sourceMaxTS.IsZero() && (targetMaxTS.Equal(sourceMaxTS) || targetMaxTS.After(sourceMaxTS)) {
		if s.cfg.Verbose {
			log.Printf("%s: sync completed", tableName)
			log.Printf("%s: checking for deletions", tableName)
		}
		if err := s.handleDeletedRows(ctx, tableInfo); err != nil {
			return fmt.Errorf("failed to handle deleted rows: %w", err)
		}
	}

	return nil
}

// handleDeletedRows identifies and removes deleted rows from target
func (s *Syncer) handleDeletedRows(ctx context.Context, tableInfo *table.Info) error {
	if len(tableInfo.PrimaryKey) == 0 {
		if s.cfg.Verbose {
			log.Printf("%s: has no primary key, skipping deleted rows handling", tableInfo.Name)
		}
		return nil
	}

	pkCols := s.quotedColumnsList(tableInfo.PrimaryKey)

	// Get all primary keys from source
	sourceQuery := fmt.Sprintf("SELECT %s FROM %s", pkCols, s.quotedTableName(tableInfo.Name))
	sourceRows, err := s.sourceDB.QueryContext(ctx, sourceQuery)
	if err != nil {
		return fmt.Errorf("failed to query source primary keys: %w", err)
	}
	defer func() { _ = sourceRows.Close() }()

	sourcePKs := make(map[string]bool)
	for sourceRows.Next() {
		values := make([]any, len(tableInfo.PrimaryKey))
		scanArgs := make([]any, len(tableInfo.PrimaryKey))
		for i := range values {
			scanArgs[i] = &values[i]
		}

		if err := sourceRows.Scan(scanArgs...); err != nil {
			return fmt.Errorf("failed to scan source primary key: %w", err)
		}

		// Create a composite key string
		pkStr := s.createPKString(values)
		sourcePKs[pkStr] = true
	}

	// Get all primary keys from target and identify deletions
	targetQuery := fmt.Sprintf("SELECT %s FROM %s", pkCols, s.quotedTableName(tableInfo.Name))
	targetRows, err := s.targetDB.QueryContext(ctx, targetQuery)
	if err != nil {
		return fmt.Errorf("failed to query target primary keys: %w", err)
	}
	defer func() { _ = targetRows.Close() }()

	var toDelete [][]any
	for targetRows.Next() {
		values := make([]any, len(tableInfo.PrimaryKey))
		scanArgs := make([]any, len(tableInfo.PrimaryKey))
		for i := range values {
			scanArgs[i] = &values[i]
		}

		if err := targetRows.Scan(scanArgs...); err != nil {
			return fmt.Errorf("failed to scan target primary key: %w", err)
		}

		pkStr := s.createPKString(values)
		if !sourcePKs[pkStr] {
			toDelete = append(toDelete, values)
			// Chunked deletion using batch size to avoid large statements
			if s.cfg.BatchSize > 0 && len(toDelete) >= s.cfg.BatchSize {
				if s.cfg.Verbose {
					log.Printf("%s: deleting %d rows (chunk)", tableInfo.Name, len(toDelete))
				}
				if err := s.deleteRows(ctx, tableInfo, toDelete); err != nil {
					return fmt.Errorf("failed to delete rows chunk: %w", err)
				}
				s.addDeletes(tableInfo.Name, len(toDelete))
				toDelete = toDelete[:0]
			}
		}
	}

	// Delete rows that exist in target but not in source
	if len(toDelete) > 0 {
		if s.cfg.Verbose {
			log.Printf("%s: deleting %d rows", tableInfo.Name, len(toDelete))
		}

		if err := s.deleteRows(ctx, tableInfo, toDelete); err != nil {
			return fmt.Errorf("failed to delete rows: %w", err)
		}

		s.addDeletes(tableInfo.Name, len(toDelete))
	}

	return nil
}

// deleteAllInChunks deletes all rows from target table in chunks (by PK if available)
func (s *Syncer) deleteAllInChunks(ctx context.Context, tableInfo *table.Info) error {
	if len(tableInfo.PrimaryKey) == 0 {
		// Fall back to a single DELETE FROM table
		query := fmt.Sprintf("DELETE FROM %s", s.quotedTableName(tableInfo.Name))
		res, err := s.targetDB.ExecContext(ctx, query)
		if err != nil {
			return err
		}
		if n, err := res.RowsAffected(); err == nil {
			s.addDeletes(tableInfo.Name, int(n))
		}
		return nil
	}

	pkCols := s.quotedColumnsList(tableInfo.PrimaryKey)
	batch := s.cfg.BatchSize
	if batch <= 0 {
		batch = 1000
	}

	for {
		selectQuery := fmt.Sprintf("SELECT %s FROM %s LIMIT %d", pkCols, s.quotedTableName(tableInfo.Name), batch)
		rows, err := s.targetDB.QueryContext(ctx, selectQuery)
		if err != nil {
			return err
		}
		var pkRows [][]any
		for rows.Next() {
			values := make([]any, len(tableInfo.PrimaryKey))
			scanArgs := make([]any, len(tableInfo.PrimaryKey))
			for i := range values {
				scanArgs[i] = &values[i]
			}
			if err := rows.Scan(scanArgs...); err != nil {
				_ = rows.Close()
				return err
			}
			pkRows = append(pkRows, values)
		}
		_ = rows.Close()

		if len(pkRows) == 0 {
			break
		}

		if s.cfg.Verbose {
			log.Printf("%s: deleting %d rows", tableInfo.Name, len(pkRows))
		}
		if err := s.deleteRows(ctx, tableInfo, pkRows); err != nil {
			return err
		}
		s.addDeletes(tableInfo.Name, len(pkRows))

		if len(pkRows) < batch {
			break
		}
	}
	return nil
}

// tableHasRows checks if a table has at least one row
func (s *Syncer) tableHasRows(ctx context.Context, dbh *sql.DB, tableName string) (bool, error) {
	query := fmt.Sprintf("SELECT EXISTS (SELECT 1 FROM %s LIMIT 1)", s.quotedTableName(tableName))
	var exists bool
	if err := dbh.QueryRowContext(ctx, query).Scan(&exists); err != nil {
		return false, err
	}
	return exists, nil
}

// createPKString creates a string representation of primary key values
func (s *Syncer) createPKString(values []any) string {
	parts := make([]string, len(values))
	for i, v := range values {
		parts[i] = fmt.Sprintf("%v", v)
	}
	return strings.Join(parts, "|")
}

// deleteRows deletes specified rows from target table
func (s *Syncer) deleteRows(ctx context.Context, tableInfo *table.Info, rows [][]any) error {
	if len(rows) == 0 {
		return nil
	}

	// Build WHERE clause for primary key
	whereClause := s.buildPKWhereClause(tableInfo.PrimaryKey)
	deleteQuery := fmt.Sprintf("DELETE FROM %s WHERE %s", s.quotedTableName(tableInfo.Name), whereClause)

	stmt, err := s.targetDB.PrepareContext(ctx, deleteQuery)
	if err != nil {
		return fmt.Errorf("failed to prepare delete statement: %w", err)
	}
	defer func() { _ = stmt.Close() }()

	for _, row := range rows {
		if _, err := stmt.ExecContext(ctx, row...); err != nil {
			return fmt.Errorf("failed to delete row: %w", err)
		}
	}

	return nil
}

// buildPKWhereClause builds a WHERE clause for primary key matching
func (s *Syncer) buildPKWhereClause(pkCols []string) string {
	parts := make([]string, len(pkCols))
	for i, col := range pkCols {
		parts[i] = fmt.Sprintf("%s = $%d", s.quotedColumnName(col), i+1)
	}
	return strings.Join(parts, " AND ")
}

// getTimestampRange gets the range of timestamps to process
func (s *Syncer) getTimestampRange(ctx context.Context, tableName string, lastSync time.Time) (time.Time, time.Time, error) {
	query := fmt.Sprintf(`
		SELECT MIN(%s) as min_ts, MAX(%s) as max_ts 
		FROM %s 
		WHERE %s > $1`,
		s.quotedColumnName(s.cfg.TimestampCol), s.quotedColumnName(s.cfg.TimestampCol),
		s.quotedTableName(tableName), s.quotedColumnName(s.cfg.TimestampCol))

	var minTS, maxTS sql.NullTime
	err := s.sourceDB.QueryRowContext(ctx, query, lastSync).Scan(&minTS, &maxTS)
	if err != nil {
		return time.Time{}, time.Time{}, err
	}

	if !minTS.Valid || !maxTS.Valid {
		return time.Time{}, time.Time{}, nil // No new data
	}

	return minTS.Time, maxTS.Time, nil
}

// processBatch processes a batch of data for a given timestamp range
func (s *Syncer) processBatch(ctx context.Context, tableInfo *table.Info, fromTS, maxTS time.Time) (time.Time, error) {
	// Calculate the batch end timestamp
	batchEndTS := s.calculateBatchEndTimestamp(ctx, tableInfo, fromTS, maxTS)

	// Ensure we get all rows with the same timestamp as batchEndTS
	// This is crucial for handling bulk updates where multiple rows have the same timestamp
	actualEndTS, err := s.getActualBatchEndTimestamp(ctx, tableInfo, fromTS, batchEndTS)
	if err != nil {
		return time.Time{}, err
	}

	if s.cfg.Verbose {
		log.Printf("%s: processing batch from %s", tableInfo.Name, fromTS.Format(time.RFC3339))
	}

	// Get data from source
	rows, err := s.getSourceData(ctx, tableInfo, fromTS, actualEndTS)
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to get source data: %w", err)
	}

	if len(rows) == 0 {
		return actualEndTS, nil
	}

	// Upsert data to target
	if err := s.upsertData(ctx, tableInfo, rows); err != nil {
		return time.Time{}, fmt.Errorf("failed to upsert data: %w", err)
	}

	return actualEndTS, nil
}

// calculateBatchEndTimestamp calculates the end timestamp for the current batch
func (s *Syncer) calculateBatchEndTimestamp(ctx context.Context, tableInfo *table.Info, fromTS, maxTS time.Time) time.Time {
	// Try to process up to batchSize rows
	query := fmt.Sprintf(`
		SELECT %s 
		FROM %s 
		WHERE %s >= $1 AND %s <= $2 
		ORDER BY %s 
		LIMIT $3`,
		s.quotedColumnName(s.cfg.TimestampCol), s.quotedTableName(tableInfo.Name),
		s.quotedColumnName(s.cfg.TimestampCol), s.quotedColumnName(s.cfg.TimestampCol),
		s.quotedColumnName(s.cfg.TimestampCol))

	rows, err := s.sourceDB.QueryContext(ctx, query, fromTS, maxTS, s.cfg.BatchSize)
	if err != nil {
		return maxTS // Fallback to max timestamp
	}
	defer func() { _ = rows.Close() }()

	var lastTS time.Time
	count := 0
	for rows.Next() {
		count++
		if err := rows.Scan(&lastTS); err != nil {
			return maxTS
		}
	}

	if count < s.cfg.BatchSize {
		return maxTS // We got all remaining data
	}

	return lastTS
}

// getActualBatchEndTimestamp ensures we include all rows with the same timestamp
func (s *Syncer) getActualBatchEndTimestamp(ctx context.Context, tableInfo *table.Info, fromTS, batchEndTS time.Time) (time.Time, error) {
	// Find the next timestamp after batchEndTS
	query := fmt.Sprintf(`
		SELECT MIN(%s) 
		FROM %s 
		WHERE %s > $1`,
		s.quotedColumnName(s.cfg.TimestampCol), s.quotedTableName(tableInfo.Name),
		s.quotedColumnName(s.cfg.TimestampCol))

	var nextTS sql.NullTime
	err := s.sourceDB.QueryRowContext(ctx, query, batchEndTS).Scan(&nextTS)
	if err != nil {
		return batchEndTS, nil // Use the original batch end
	}

	if !nextTS.Valid {
		return batchEndTS, nil // No more data after this timestamp
	}

	// Return the timestamp just before the next one
	return nextTS.Time.Add(-time.Microsecond), nil
}

// getSourceData retrieves data from source table within timestamp range
func (s *Syncer) getSourceData(ctx context.Context, tableInfo *table.Info, fromTS, toTS time.Time) ([][]any, error) {
	columns := s.quotedColumnsList(tableInfo.Columns)
	query := fmt.Sprintf(`
		SELECT %s 
		FROM %s 
		WHERE %s >= $1 AND %s <= $2 
		ORDER BY %s`,
		columns, s.quotedTableName(tableInfo.Name),
		s.quotedColumnName(s.cfg.TimestampCol), s.quotedColumnName(s.cfg.TimestampCol),
		s.quotedColumnName(s.cfg.TimestampCol))

	rows, err := s.sourceDB.QueryContext(ctx, query, fromTS, toTS)
	if err != nil {
		return nil, err
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
			return nil, err
		}

		result = append(result, values)
	}

	return result, nil
}

// upsertData performs upsert operation on target table
func (s *Syncer) upsertData(ctx context.Context, tableInfo *table.Info, rows [][]any) error {
	if len(rows) == 0 {
		return nil
	}

	upsertQuery := s.buildUpsertQuery(tableInfo)
	stmt, err := s.targetDB.PrepareContext(ctx, upsertQuery)
	if err != nil {
		return fmt.Errorf("failed to prepare upsert statement: %w", err)
	}
	defer func() { _ = stmt.Close() }()

	for _, row := range rows {
		if _, err := stmt.ExecContext(ctx, row...); err != nil {
			return fmt.Errorf("failed to upsert row: %w", err)
		}
	}

	s.addUpserts(tableInfo.Name, len(rows))

	return nil
}

// buildUpsertQuery builds an upsert query for PostgreSQL
func (s *Syncer) buildUpsertQuery(tableInfo *table.Info) string {
	columns := s.quotedColumnsList(tableInfo.Columns)
	placeholders := make([]string, len(tableInfo.Columns))
	for i := range placeholders {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
	}
	placeholderStr := strings.Join(placeholders, ", ")

	// Build ON CONFLICT clause
	conflictCols := s.quotedColumnsList(tableInfo.PrimaryKey)

	// Build UPDATE SET clause (exclude primary key columns)
	var updateParts []string
	for _, col := range tableInfo.Columns {
		isPK := false
		for _, pk := range tableInfo.PrimaryKey {
			if col == pk {
				isPK = true
				break
			}
		}
		if !isPK {
			quotedCol := s.quotedColumnName(col)
			updateParts = append(updateParts, fmt.Sprintf("%s = EXCLUDED.%s", quotedCol, quotedCol))
		}
	}
	updateClause := strings.Join(updateParts, ", ")

	query := fmt.Sprintf(`
		INSERT INTO %s (%s) 
		VALUES (%s) 
		ON CONFLICT (%s) 
		DO UPDATE SET %s`,
		s.quotedTableName(tableInfo.Name), columns, placeholderStr, conflictCols, updateClause)

	return query
}

// tableCounts returns current counts for a table
func (s *Syncer) tableCounts(table string) (int64, int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.upsertsByTable[table], s.deletesByTable[table]
}

func (s *Syncer) addUpserts(table string, n int) {
	if n <= 0 {
		return
	}
	s.mu.Lock()
	s.upsertsByTable[table] += int64(n)
	s.totalUpserts += int64(n)
	s.mu.Unlock()
}

func (s *Syncer) addDeletes(table string, n int) {
	if n <= 0 {
		return
	}
	s.mu.Lock()
	s.deletesByTable[table] += int64(n)
	s.totalDeletes += int64(n)
	s.mu.Unlock()
}

func (s *Syncer) addSkipped(table string) {
	s.mu.Lock()
	s.skippedTables = append(s.skippedTables, table)
	s.mu.Unlock()
}

// writeIntegrityCSV writes integrity stats for each table into integrity.csv
func (s *Syncer) writeIntegrityCSV(ctx context.Context, tables []*table.Info) error {
	// Create file in current working directory
	f, err := os.Create("integrity.csv")
	if err != nil {
		return err
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
		return err
	}

	for _, info := range tables {
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
		} else {
			// leave as empty strings if no suitable id column
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
			return err
		}
	}

	w.Flush()
	return w.Error()
}

// getExactRowCount returns exact COUNT(*) from the given DB for a table
func (s *Syncer) getExactRowCount(ctx context.Context, dbh *sql.DB, tableName string) (int64, error) {
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s", s.quotedTableName(tableName))
	var cnt int64
	if err := dbh.QueryRowContext(ctx, query).Scan(&cnt); err != nil {
		return 0, err
	}
	return cnt, nil
}

// getMinMaxAsText returns MIN and MAX for a column, cast to text for CSV friendliness
func (s *Syncer) getMinMaxAsText(ctx context.Context, dbh *sql.DB, tableName, column string) (string, string, error) {
	// Cast the column to text BEFORE applying MIN/MAX to support types without direct aggregates (e.g., uuid)
	query := fmt.Sprintf("SELECT MIN((%s)::text), MAX((%s)::text) FROM %s", s.quotedColumnName(column), s.quotedColumnName(column), s.quotedTableName(tableName))
	var minVal, maxVal sql.NullString
	if err := dbh.QueryRowContext(ctx, query).Scan(&minVal, &maxVal); err != nil {
		return "", "", err
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

// findIDColumn determines the best ID column to use for min/max: prefers single-column PK, falls back to a column named 'id'
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
