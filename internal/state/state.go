package state

import (
	"database/sql"
	"fmt"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// StateDB manages sync state using SQLite
type StateDB struct {
	db *sql.DB
}

// New creates a new StateDB instance
func New(dbPath string) (*StateDB, error) {
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open SQLite database: %w", err)
	}

	stateDB := &StateDB{db: db}

	if err := stateDB.init(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to initialize state database: %w", err)
	}

	return stateDB, nil
}

// Close closes the database connection
func (s *StateDB) Close() error {
	return s.db.Close()
}

// init creates necessary tables
func (s *StateDB) init() error {
	query := `
		CREATE TABLE IF NOT EXISTS sync_state (
			table_name TEXT PRIMARY KEY,
			last_sync_timestamp DATETIME,
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
		);

		CREATE TABLE IF NOT EXISTS sync_log (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			table_name TEXT NOT NULL,
			sync_type TEXT NOT NULL,
			rows_processed INTEGER DEFAULT 0,
			start_time DATETIME,
			end_time DATETIME,
			status TEXT DEFAULT 'running',
			error_message TEXT,
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP
		);

		CREATE INDEX IF NOT EXISTS idx_sync_log_table_time ON sync_log(table_name, start_time);
		CREATE INDEX IF NOT EXISTS idx_sync_log_status ON sync_log(status);
	`

	_, err := s.db.Exec(query)
	return err
}

// GetLastSync returns the last sync timestamp for a table
func (s *StateDB) GetLastSync(tableName string) (time.Time, error) {
	query := "SELECT last_sync_timestamp FROM sync_state WHERE table_name = ?"

	var timestamp sql.NullTime
	err := s.db.QueryRow(query, tableName).Scan(&timestamp)

	if err == sql.ErrNoRows {
		// No previous sync found, return zero time
		return time.Time{}, nil
	}
	if err != nil {
		return time.Time{}, err
	}

	if timestamp.Valid {
		return timestamp.Time, nil
	}

	return time.Time{}, nil
}

// SetLastSync updates the last sync timestamp for a table
func (s *StateDB) SetLastSync(tableName string, timestamp time.Time) error {
	query := `
		INSERT INTO sync_state (table_name, last_sync_timestamp, updated_at) 
		VALUES (?, ?, CURRENT_TIMESTAMP)
		ON CONFLICT(table_name) DO UPDATE SET 
			last_sync_timestamp = excluded.last_sync_timestamp,
			updated_at = excluded.updated_at
	`

	_, err := s.db.Exec(query, tableName, timestamp)
	return err
}

// LogSyncStart logs the start of a sync operation
func (s *StateDB) LogSyncStart(tableName, syncType string) (int64, error) {
	query := `
		INSERT INTO sync_log (table_name, sync_type, start_time, status) 
		VALUES (?, ?, CURRENT_TIMESTAMP, 'running')
	`

	result, err := s.db.Exec(query, tableName, syncType)
	if err != nil {
		return 0, err
	}

	return result.LastInsertId()
}

// LogSyncEnd logs the completion of a sync operation
func (s *StateDB) LogSyncEnd(logID int64, rowsProcessed int, status string, errorMessage string) error {
	query := `
		UPDATE sync_log 
		SET end_time = CURRENT_TIMESTAMP, 
		    rows_processed = ?, 
		    status = ?, 
		    error_message = ?
		WHERE id = ?
	`

	_, err := s.db.Exec(query, rowsProcessed, status, errorMessage, logID)
	return err
}

// GetSyncHistory returns sync history for a table
func (s *StateDB) GetSyncHistory(tableName string, limit int) ([]SyncLogEntry, error) {
	query := `
		SELECT id, table_name, sync_type, rows_processed, start_time, end_time, status, error_message
		FROM sync_log 
		WHERE table_name = ? 
		ORDER BY start_time DESC 
		LIMIT ?
	`

	rows, err := s.db.Query(query, tableName, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var entries []SyncLogEntry
	for rows.Next() {
		var entry SyncLogEntry
		var startTime, endTime sql.NullTime
		var errorMessage sql.NullString

		err := rows.Scan(
			&entry.ID,
			&entry.TableName,
			&entry.SyncType,
			&entry.RowsProcessed,
			&startTime,
			&endTime,
			&entry.Status,
			&errorMessage,
		)
		if err != nil {
			return nil, err
		}

		if startTime.Valid {
			entry.StartTime = startTime.Time
		}
		if endTime.Valid {
			entry.EndTime = &endTime.Time
		}
		if errorMessage.Valid {
			entry.ErrorMessage = &errorMessage.String
		}

		entries = append(entries, entry)
	}

	return entries, nil
}

// CleanupOldLogs removes old log entries
func (s *StateDB) CleanupOldLogs(olderThanDays int) error {
	query := `
		DELETE FROM sync_log 
		WHERE start_time < datetime('now', '-' || ? || ' days')
	`

	_, err := s.db.Exec(query, olderThanDays)
	return err
}

// SyncLogEntry represents a sync log entry
type SyncLogEntry struct {
	ID            int64      `json:"id"`
	TableName     string     `json:"table_name"`
	SyncType      string     `json:"sync_type"`
	RowsProcessed int        `json:"rows_processed"`
	StartTime     time.Time  `json:"start_time"`
	EndTime       *time.Time `json:"end_time,omitempty"`
	Status        string     `json:"status"`
	ErrorMessage  *string    `json:"error_message,omitempty"`
}
