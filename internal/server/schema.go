package server

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/koltyakov/pgsync/internal/db"
	_ "github.com/lib/pq"
)

// TableInfo represents table metadata for the UI
type TableInfo struct {
	Name           string       `json:"name"`
	Columns        []ColumnInfo `json:"columns"`
	PrimaryKey     []string     `json:"primaryKey"`
	RowCount       int64        `json:"rowCount"`
	SourceRowCount int64        `json:"sourceRowCount"`
	TargetRowCount int64        `json:"targetRowCount"`
	ExistsInTarget bool         `json:"existsInTarget"`
}

// ColumnInfo represents column metadata
type ColumnInfo struct {
	Name              string `json:"name"`
	DataType          string `json:"dataType"`
	IsNullable        bool   `json:"isNullable"`
	IsPrimary         bool   `json:"isPrimary"`
	IsForeignKey      bool   `json:"isForeignKey"`
	FKReferencesTable string `json:"fkReferencesTable,omitempty"`
	ExistsInTarget    bool   `json:"existsInTarget"`
}

// SchemaResponse wraps the list of tables
type SchemaResponse struct {
	Tables []string `json:"tables"`
	Schema string   `json:"schema"`
}

// handleGetTables returns all tables in the schema
func (s *Server) handleGetTables(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	sourceDB, err := sql.Open("postgres", s.sourceDB)
	if err != nil {
		s.writeError(w, "Failed to connect to source database", err, http.StatusInternalServerError)
		return
	}
	defer sourceDB.Close()

	inspector := db.NewInspector(sourceDB, nil, s.schema)
	tables, err := inspector.GetTables(ctx)
	if err != nil {
		s.writeError(w, "Failed to get tables", err, http.StatusInternalServerError)
		return
	}

	s.writeJSON(w, SchemaResponse{
		Tables: tables,
		Schema: s.schema,
	})
}

// handleGetTableInfo returns detailed info for a specific table
func (s *Server) handleGetTableInfo(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Extract table name from path: /api/schema/table/{tableName}
	tableName := strings.TrimPrefix(r.URL.Path, "/api/schema/table/")
	if tableName == "" {
		http.Error(w, "Table name required", http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	sourceDB, err := sql.Open("postgres", s.sourceDB)
	if err != nil {
		s.writeError(w, "Failed to connect to source database", err, http.StatusInternalServerError)
		return
	}
	defer sourceDB.Close()

	targetDB, err := sql.Open("postgres", s.targetDB)
	if err != nil {
		s.writeError(w, "Failed to connect to target database", err, http.StatusInternalServerError)
		return
	}
	defer targetDB.Close()

	sourceInspector := db.NewInspector(sourceDB, nil, s.schema)
	info, err := sourceInspector.GetTableInfo(ctx, tableName)
	if err != nil {
		s.writeError(w, "Failed to get table info", err, http.StatusInternalServerError)
		return
	}

	// Check if table exists in target and get target info
	targetInspector := db.NewInspector(targetDB, nil, s.schema)
	targetInfo, targetErr := targetInspector.GetTableInfo(ctx, tableName)
	existsInTarget := targetErr == nil
	var targetRowCount int64
	var targetColumns map[string]bool
	if existsInTarget {
		targetRowCount = targetInfo.RowCount
		targetColumns = make(map[string]bool)
		for _, col := range targetInfo.Columns {
			targetColumns[col] = true
		}
	}

	// Get detailed column info from source
	columns, err := s.getDetailedColumns(ctx, sourceDB, tableName)
	if err != nil {
		s.writeError(w, "Failed to get column details", err, http.StatusInternalServerError)
		return
	}

	// Mark primary key columns and target existence
	pkSet := make(map[string]bool)
	for _, pk := range info.PrimaryKey {
		pkSet[pk] = true
	}
	for i := range columns {
		columns[i].IsPrimary = pkSet[columns[i].Name]
		columns[i].ExistsInTarget = existsInTarget && targetColumns[columns[i].Name]
	}

	s.writeJSON(w, TableInfo{
		Name:           info.Name,
		Columns:        columns,
		PrimaryKey:     info.PrimaryKey,
		RowCount:       info.RowCount,
		SourceRowCount: info.RowCount,
		TargetRowCount: targetRowCount,
		ExistsInTarget: existsInTarget,
	})
}

// getDetailedColumns returns column info with data types
func (s *Server) getDetailedColumns(ctx context.Context, db *sql.DB, tableName string) ([]ColumnInfo, error) {
	// Query columns with formatted data types
	query := `
		SELECT 
			column_name,
			LOWER(CASE 
				WHEN data_type = 'character varying' THEN 
					CASE WHEN character_maximum_length IS NOT NULL 
						THEN 'varchar(' || character_maximum_length || ')'
						ELSE 'varchar'
					END
				WHEN data_type = 'character' THEN 
					CASE WHEN character_maximum_length IS NOT NULL 
						THEN 'char(' || character_maximum_length || ')'
						ELSE 'char'
					END
				WHEN data_type = 'numeric' THEN 
					CASE WHEN numeric_precision IS NOT NULL 
						THEN 'numeric(' || numeric_precision || ',' || COALESCE(numeric_scale, 0) || ')'
						ELSE 'numeric'
					END
				WHEN data_type = 'timestamp with time zone' THEN 'timestamptz'
				WHEN data_type = 'timestamp without time zone' THEN 'timestamp'
				WHEN data_type = 'time with time zone' THEN 'timetz'
				WHEN data_type = 'time without time zone' THEN 'time'
				WHEN data_type = 'double precision' THEN 'float8'
				WHEN data_type = 'real' THEN 'float4'
				WHEN data_type = 'smallint' THEN 'int2'
				WHEN data_type = 'integer' THEN 'int4'
				WHEN data_type = 'bigint' THEN 'int8'
				WHEN data_type = 'boolean' THEN 'bool'
				ELSE data_type
			END) as formatted_type,
			is_nullable
		FROM information_schema.columns
		WHERE table_schema = $1 AND table_name = $2
		ORDER BY ordinal_position`

	rows, err := db.QueryContext(ctx, query, s.schema, tableName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var columns []ColumnInfo
	for rows.Next() {
		var col ColumnInfo
		var nullable string
		if err := rows.Scan(&col.Name, &col.DataType, &nullable); err != nil {
			return nil, err
		}
		col.IsNullable = nullable == "YES"
		columns = append(columns, col)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	// Get foreign key columns
	fkMap, err := s.getForeignKeyColumns(ctx, db, tableName)
	if err != nil {
		// Log but don't fail - FK info is nice to have
		s.logger.Warn("Failed to get FK info", "table", tableName, "error", err)
	} else {
		for i := range columns {
			if refTable, ok := fkMap[columns[i].Name]; ok {
				columns[i].IsForeignKey = true
				columns[i].FKReferencesTable = refTable
			}
		}
	}

	return columns, nil
}

// getForeignKeyColumns returns a map of column names to their referenced table
func (s *Server) getForeignKeyColumns(ctx context.Context, db *sql.DB, tableName string) (map[string]string, error) {
	query := `
		SELECT kcu.column_name, ccu.table_name AS referenced_table
		FROM information_schema.table_constraints tc
		JOIN information_schema.key_column_usage kcu 
			ON tc.constraint_name = kcu.constraint_name 
			AND tc.table_schema = kcu.table_schema
		JOIN information_schema.constraint_column_usage ccu
			ON tc.constraint_name = ccu.constraint_name
			AND tc.table_schema = ccu.table_schema
		WHERE tc.constraint_type = 'FOREIGN KEY'
			AND tc.table_schema = $1
			AND tc.table_name = $2`

	rows, err := db.QueryContext(ctx, query, s.schema, tableName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	fkMap := make(map[string]string)
	for rows.Next() {
		var colName, refTable string
		if err := rows.Scan(&colName, &refTable); err != nil {
			return nil, err
		}
		fkMap[colName] = refTable
	}
	return fkMap, rows.Err()
}

// writeJSON writes a JSON response
func (s *Server) writeJSON(w http.ResponseWriter, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(data); err != nil {
		s.logger.Error("Failed to encode JSON response", "error", err)
	}
}

// writeError writes an error response
func (s *Server) writeError(w http.ResponseWriter, message string, err error, status int) {
	s.logger.Error(message, "error", err)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]string{
		"error":   message,
		"details": err.Error(),
	})
}
