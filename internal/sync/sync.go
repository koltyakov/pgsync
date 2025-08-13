package sync

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/koltyakov/pgsync/internal/config"
	"github.com/koltyakov/pgsync/internal/db"
	"github.com/koltyakov/pgsync/internal/state"
	"github.com/koltyakov/pgsync/internal/table"
	_ "github.com/lib/pq"
)

// Syncer handles the synchronization between two PostgreSQL databases
type Syncer struct {
	cfg       *config.Config
	sourceDB  *sql.DB
	targetDB  *sql.DB
	stateDB   *state.StateDB
	inspector *db.Inspector
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

// New creates a new Syncer instance
func New(cfg *config.Config) (*Syncer, error) {
	sourceDB, err := sql.Open("postgres", cfg.SourceDB)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to source database: %w", err)
	}

	targetDB, err := sql.Open("postgres", cfg.TargetDB)
	if err != nil {
		sourceDB.Close()
		return nil, fmt.Errorf("failed to connect to target database: %w", err)
	}

	// Test connections
	if err := sourceDB.Ping(); err != nil {
		sourceDB.Close()
		targetDB.Close()
		return nil, fmt.Errorf("failed to ping source database: %w", err)
	}

	if err := targetDB.Ping(); err != nil {
		sourceDB.Close()
		targetDB.Close()
		return nil, fmt.Errorf("failed to ping target database: %w", err)
	}

	stateDB, err := state.New(cfg.StateDB)
	if err != nil {
		sourceDB.Close()
		targetDB.Close()
		return nil, fmt.Errorf("failed to initialize state database: %w", err)
	}

	inspector := db.NewInspector(sourceDB, targetDB, cfg.Schema)

	return &Syncer{
		cfg:       cfg,
		sourceDB:  sourceDB,
		targetDB:  targetDB,
		stateDB:   stateDB,
		inspector: inspector,
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
	if err := s.stateDB.Close(); err != nil {
		errs = append(errs, fmt.Sprintf("state DB: %v", err))
	}

	if len(errs) > 0 {
		return fmt.Errorf("errors closing databases: %s", strings.Join(errs, ", "))
	}
	return nil
}

// Sync performs the synchronization process
func (s *Syncer) Sync() error {
	ctx := context.Background()

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
		log.Printf("Found %d tables to sync: %v", len(tables), tables)
	}

	// Get table metadata for all tables
	tableInfos := make([]*table.Info, 0, len(tables))
	for _, tableName := range tables {
		info, err := s.inspector.GetTableInfo(ctx, tableName)
		if err != nil {
			log.Printf("Warning: failed to get info for table %s: %v", tableName, err)
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

	log.Println("All table syncs completed")
	return nil
}

// worker processes table sync jobs
func (s *Syncer) worker(ctx context.Context, workerID int, workChan <-chan *table.Info) {
	for tableInfo := range workChan {
		if s.cfg.Verbose {
			log.Printf("Worker %d: Starting sync for table %s", workerID, tableInfo.Name)
		}

		if err := s.syncTable(ctx, tableInfo); err != nil {
			log.Printf("Worker %d: Error syncing table %s: %v", workerID, tableInfo.Name, err)
		} else if s.cfg.Verbose {
			log.Printf("Worker %d: Completed sync for table %s", workerID, tableInfo.Name)
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
		// Filter to only included tables
		includeMap := make(map[string]bool)
		for _, table := range s.cfg.IncludeTables {
			includeMap[strings.TrimSpace(table)] = true
		}

		var filtered []string
		for _, table := range allTables {
			if includeMap[table] {
				filtered = append(filtered, table)
			}
		}
		return filtered, nil
	}

	if len(s.cfg.ExcludeTables) > 0 {
		// Filter out excluded tables
		excludeMap := make(map[string]bool)
		for _, table := range s.cfg.ExcludeTables {
			excludeMap[strings.TrimSpace(table)] = true
		}

		var filtered []string
		for _, table := range allTables {
			if !excludeMap[table] {
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

	// Get last sync timestamp from state
	lastSync, err := s.stateDB.GetLastSync(tableName)
	if err != nil {
		return fmt.Errorf("failed to get last sync timestamp: %w", err)
	}

	// Check if table has timestamp column
	hasTimestamp := tableInfo.HasColumn(s.cfg.TimestampCol)

	if !hasTimestamp {
		log.Printf("Table %s has no %s column, skipping sync", tableName, s.cfg.TimestampCol)
		return nil
	}

	// If this is the first sync (lastSync is zero), initialize from target DB
	if lastSync.IsZero() {
		targetMaxTimestamp, err := s.getMaxTimestampFromTarget(ctx, tableName)
		if err != nil {
			return fmt.Errorf("failed to get max timestamp from target: %w", err)
		}
		if !targetMaxTimestamp.IsZero() {
			lastSync = targetMaxTimestamp
			if s.cfg.Verbose {
				log.Printf("Initialized last sync timestamp for table %s from target DB: %v", tableName, lastSync)
			}
		}
	}
	return s.syncTableIncremental(ctx, tableInfo, lastSync)
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

// syncTableIncremental performs incremental sync based on timestamp
func (s *Syncer) syncTableIncremental(ctx context.Context, tableInfo *table.Info, lastSync time.Time) error {
	tableName := tableInfo.Name

	// First, handle deleted rows
	if err := s.handleDeletedRows(ctx, tableInfo); err != nil {
		return fmt.Errorf("failed to handle deleted rows: %w", err)
	}

	// Get the range of timestamps to process
	minTS, maxTS, err := s.getTimestampRange(ctx, tableName, lastSync)
	if err != nil {
		return fmt.Errorf("failed to get timestamp range: %w", err)
	}

	if minTS.IsZero() {
		if s.cfg.Verbose {
			log.Printf("No new data found for table %s", tableName)
		}
		return nil
	}

	if s.cfg.Verbose {
		log.Printf("Syncing table %s from %v to %v", tableName, minTS, maxTS)
	}

	// Process data in batches
	currentTS := minTS
	for currentTS.Before(maxTS) || currentTS.Equal(maxTS) {
		nextTS, err := s.processBatch(ctx, tableInfo, currentTS, maxTS)
		if err != nil {
			return fmt.Errorf("failed to process batch: %w", err)
		}

		// Update last sync timestamp
		if err := s.stateDB.SetLastSync(tableName, nextTS); err != nil {
			return fmt.Errorf("failed to update last sync timestamp: %w", err)
		}

		currentTS = nextTS.Add(time.Microsecond) // Move slightly forward to avoid duplicate processing
	}

	return nil
}

// syncTableFull performs full table comparison and sync
func (s *Syncer) syncTableFull(ctx context.Context, tableInfo *table.Info) error {
	// Handle deleted rows
	if err := s.handleDeletedRows(ctx, tableInfo); err != nil {
		return fmt.Errorf("failed to handle deleted rows: %w", err)
	}

	// For tables without timestamp, we need to compare all data
	// This is more expensive but necessary for consistency
	return s.compareAndSyncFullTable(ctx, tableInfo)
}

// handleDeletedRows identifies and removes deleted rows from target
func (s *Syncer) handleDeletedRows(ctx context.Context, tableInfo *table.Info) error {
	if len(tableInfo.PrimaryKey) == 0 {
		if s.cfg.Verbose {
			log.Printf("Table %s has no primary key, skipping deleted rows handling", tableInfo.Name)
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
	defer sourceRows.Close()

	sourcePKs := make(map[string]bool)
	for sourceRows.Next() {
		values := make([]interface{}, len(tableInfo.PrimaryKey))
		scanArgs := make([]interface{}, len(tableInfo.PrimaryKey))
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
	defer targetRows.Close()

	var toDelete [][]interface{}
	for targetRows.Next() {
		values := make([]interface{}, len(tableInfo.PrimaryKey))
		scanArgs := make([]interface{}, len(tableInfo.PrimaryKey))
		for i := range values {
			scanArgs[i] = &values[i]
		}

		if err := targetRows.Scan(scanArgs...); err != nil {
			return fmt.Errorf("failed to scan target primary key: %w", err)
		}

		pkStr := s.createPKString(values)
		if !sourcePKs[pkStr] {
			toDelete = append(toDelete, values)
		}
	}

	// Delete rows that exist in target but not in source
	if len(toDelete) > 0 {
		if s.cfg.Verbose {
			log.Printf("Deleting %d rows from table %s", len(toDelete), tableInfo.Name)
		}

		if !s.cfg.DryRun {
			if err := s.deleteRows(ctx, tableInfo, toDelete); err != nil {
				return fmt.Errorf("failed to delete rows: %w", err)
			}
		}
	}

	return nil
}

// createPKString creates a string representation of primary key values
func (s *Syncer) createPKString(values []interface{}) string {
	parts := make([]string, len(values))
	for i, v := range values {
		parts[i] = fmt.Sprintf("%v", v)
	}
	return strings.Join(parts, "|")
}

// deleteRows deletes specified rows from target table
func (s *Syncer) deleteRows(ctx context.Context, tableInfo *table.Info, rows [][]interface{}) error {
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
	defer stmt.Close()

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
		log.Printf("Processing batch for table %s from %v to %v", tableInfo.Name, fromTS, actualEndTS)
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
	if !s.cfg.DryRun {
		if err := s.upsertData(ctx, tableInfo, rows); err != nil {
			return time.Time{}, fmt.Errorf("failed to upsert data: %w", err)
		}
	}

	if s.cfg.Verbose {
		log.Printf("Processed %d rows for table %s", len(rows), tableInfo.Name)
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
	defer rows.Close()

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
func (s *Syncer) getSourceData(ctx context.Context, tableInfo *table.Info, fromTS, toTS time.Time) ([][]interface{}, error) {
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
	defer rows.Close()

	var result [][]interface{}
	for rows.Next() {
		values := make([]interface{}, len(tableInfo.Columns))
		scanArgs := make([]interface{}, len(tableInfo.Columns))
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
func (s *Syncer) upsertData(ctx context.Context, tableInfo *table.Info, rows [][]interface{}) error {
	if len(rows) == 0 {
		return nil
	}

	upsertQuery := s.buildUpsertQuery(tableInfo)
	stmt, err := s.targetDB.PrepareContext(ctx, upsertQuery)
	if err != nil {
		return fmt.Errorf("failed to prepare upsert statement: %w", err)
	}
	defer stmt.Close()

	for _, row := range rows {
		if _, err := stmt.ExecContext(ctx, row...); err != nil {
			return fmt.Errorf("failed to upsert row: %w", err)
		}
	}

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

// compareAndSyncFullTable performs full table comparison for tables without timestamp
func (s *Syncer) compareAndSyncFullTable(ctx context.Context, tableInfo *table.Info) error {
	// This is a simplified implementation
	// In practice, you might want to use checksums or row-by-row comparison
	log.Printf("Full table sync for %s is not yet implemented", tableInfo.Name)
	return nil
}
