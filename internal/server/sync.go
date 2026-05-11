package server

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/koltyakov/pgsync/internal/config"
	"github.com/koltyakov/pgsync/internal/syncer"
)

const (
	syncStepInitializing = "Initializing..."
	messageTypeProgress  = "progress"
	messageTypeLog       = "log"
	messageTypeError     = "error"
	messageTypeComplete  = "complete"
	messageLevelInfo     = "info"
	messageLevelDebug    = "debug"
	messageLevelWarn     = "warn"
)

// SyncRequest contains the sync configuration from the UI
type SyncRequest struct {
	Tables       []string            `json:"tables"`                 // Tables to sync (empty = all)
	Columns      map[string][]string `json:"columns"`                // Table -> columns to sync (empty = all)
	Reconcile    bool                `json:"reconcile"`              // Use reconcile mode
	DryRun       bool                `json:"dryRun"`                 // Preview only
	Parallel     int                 `json:"parallel,omitempty"`     // Parallel workers
	BatchSize    int                 `json:"batchSize,omitempty"`    // Batch size
	TimestampCol string              `json:"timestampCol,omitempty"` // Timestamp column
}

// SyncStatusResponse returns current sync status
type SyncStatusResponse struct {
	Running     bool       `json:"running"`
	StartedAt   *time.Time `json:"startedAt,omitempty"`
	CurrentStep string     `json:"currentStep,omitempty"`
	Progress    float64    `json:"progress"`
	Stats       *SyncStats `json:"stats,omitempty"`
}

// handleStartSync initiates a sync operation
func (s *Server) handleStartSync(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Parse request
	body, err := io.ReadAll(r.Body)
	if err != nil {
		s.writeError(w, "Failed to read request body", err, http.StatusBadRequest)
		return
	}

	var req SyncRequest
	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			s.writeError(w, "Invalid request body", err, http.StatusBadRequest)
			return
		}
	}

	s.mu.Lock()
	if s.syncState.Running {
		s.mu.Unlock()
		http.Error(w, "Sync already in progress", http.StatusConflict)
		return
	}
	s.syncState = &SyncState{
		Running:     true,
		StartedAt:   time.Now(),
		CurrentStep: syncStepInitializing,
		Progress:    0,
		Tables:      req.Tables,
	}
	s.mu.Unlock()

	// Start sync in background with server context for graceful shutdown
	// Note: We use serverCtx, not r.Context(), because the sync outlives the HTTP request
	go s.runSync(s.serverCtx, req)

	s.writeJSON(w, map[string]string{
		"status":  "started",
		"message": "Sync operation started",
	})
}

// handleSyncStatus returns current sync status
func (s *Server) handleSyncStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	s.mu.Lock()
	state := *s.syncState
	s.mu.Unlock()

	resp := SyncStatusResponse{
		Running:     state.Running,
		CurrentStep: state.CurrentStep,
		Progress:    state.Progress,
	}
	if !state.StartedAt.IsZero() {
		resp.StartedAt = &state.StartedAt
	}

	s.writeJSON(w, resp)
}

// runSync executes the sync operation with progress reporting
func (s *Server) runSync(parentCtx context.Context, req SyncRequest) {
	s.mu.Lock()
	if s.syncState == nil || !s.syncState.Running {
		s.syncState = &SyncState{
			Running:     true,
			StartedAt:   time.Now(),
			CurrentStep: syncStepInitializing,
			Progress:    0,
			Tables:      req.Tables,
		}
	} else {
		s.syncState.CurrentStep = syncStepInitializing
		s.syncState.Progress = 0
		s.syncState.Tables = req.Tables
	}
	s.mu.Unlock()

	s.broadcast(ProgressMessage{
		Type:    messageTypeProgress,
		Message: "Starting sync operation...",
		Level:   messageLevelInfo,
	})

	// Build config
	cfg := &config.Config{
		SourceDB:       s.sourceDB,
		TargetDB:       s.targetDB,
		Schema:         s.schema,
		Reconcile:      req.Reconcile,
		DryRun:         req.DryRun,
		Parallel:       req.Parallel,
		BatchSize:      req.BatchSize,
		TimestampCol:   req.TimestampCol,
		IncludeColumns: req.Columns,
	}

	if len(req.Tables) > 0 {
		cfg.IncludeTables = req.Tables
	}

	if err := cfg.Validate(); err != nil {
		s.syncError(fmt.Errorf("invalid configuration: %w", err))
		return
	}

	// Create progress handler
	progressHandler := &webProgressHandler{
		server:      s,
		totalTables: 0,
	}

	// Create syncer with progress callback
	syncInstance, err := syncer.NewWithProgress(cfg, progressHandler)
	if err != nil {
		s.syncError(fmt.Errorf("failed to create syncer: %w", err))
		return
	}
	defer func() { _ = syncInstance.Close() }()

	// Run sync with a context that cancels on server shutdown
	ctx, cancel := context.WithCancel(parentCtx)
	defer cancel()
	s.mu.Lock()
	s.syncCancel = cancel
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		s.syncCancel = nil
		s.mu.Unlock()
	}()

	if err := syncInstance.Sync(ctx); err != nil {
		// Don't report context cancellation as an error if server is shutting down
		if ctx.Err() != nil && parentCtx.Err() != nil {
			return
		}
		s.syncError(err)
		return
	}

	// Complete
	s.mu.Lock()
	s.syncState.Running = false
	s.syncState.CurrentStep = "Completed"
	s.syncState.Progress = 100
	s.mu.Unlock()

	stats := syncInstance.GetStats()
	s.broadcast(ProgressMessage{
		Type:     messageTypeComplete,
		Message:  "Sync completed successfully",
		Level:    messageLevelInfo,
		Progress: 100,
		Stats: &SyncStats{
			TotalUpserts: stats.TotalUpserts,
			TotalDeletes: stats.TotalDeletes,
		},
	})
}

// syncError handles sync errors
func (s *Server) syncError(err error) {
	// Don't report context cancellation as error
	if err == context.Canceled || err == context.DeadlineExceeded {
		s.mu.Lock()
		s.syncState.Running = false
		s.syncState.CurrentStep = "Cancelled"
		s.mu.Unlock()
		return
	}

	s.mu.Lock()
	s.syncState.Running = false
	s.syncState.CurrentStep = "Error"
	s.mu.Unlock()

	s.broadcast(ProgressMessage{
		Type:    messageTypeError,
		Message: err.Error(),
		Level:   messageTypeError,
	})
}

// webProgressHandler implements syncer.ProgressHandler for WebSocket updates
type webProgressHandler struct {
	server       *Server
	totalTables  int
	mu           sync.Mutex
	tableIndexes map[string]int
}

func (h *webProgressHandler) OnStart(tables []string) {
	h.mu.Lock()
	h.totalTables = len(tables)
	h.tableIndexes = make(map[string]int, len(tables))
	h.mu.Unlock()

	h.server.mu.Lock()
	h.server.syncState.Tables = tables
	h.server.mu.Unlock()

	h.server.broadcast(ProgressMessage{
		Type:        messageTypeProgress,
		Message:     fmt.Sprintf("Starting sync of %d tables", len(tables)),
		Level:       messageLevelInfo,
		TotalTables: len(tables),
	})
}

func (h *webProgressHandler) OnTableStart(table string, index int) {
	h.mu.Lock()
	if h.tableIndexes == nil {
		h.tableIndexes = make(map[string]int)
	}
	h.tableIndexes[table] = index
	totalTables := h.totalTables
	h.mu.Unlock()

	var progress float64
	if totalTables > 0 {
		progress = float64(index) / float64(totalTables) * 100
	}

	h.server.mu.Lock()
	h.server.syncState.CurrentStep = fmt.Sprintf("Syncing %s", table)
	h.server.syncState.Progress = progress
	h.server.syncState.TableIndex = index
	h.server.mu.Unlock()

	h.server.broadcast(ProgressMessage{
		Type:        messageTypeProgress,
		Message:     "Started synchronization",
		Level:       messageLevelInfo,
		Progress:    progress,
		Table:       table,
		TableIndex:  index,
		TotalTables: totalTables,
	})
}

func (h *webProgressHandler) indexForTable(table string) (int, int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.tableIndexes[table], h.totalTables
}

func (h *webProgressHandler) OnPartialSync(table string, _, ignoredCols []string, reason string) {
	tableIndex, totalTables := h.indexForTable(table)
	var colsInfo string
	if len(ignoredCols) > 5 {
		colsInfo = fmt.Sprintf("%d columns", len(ignoredCols))
	} else {
		colsInfo = strings.Join(ignoredCols, ", ")
	}
	h.server.broadcast(ProgressMessage{
		Type:        messageTypeLog,
		Message:     fmt.Sprintf("Partial sync (%s): ignoring %s", reason, colsInfo),
		Level:       messageLevelWarn,
		Table:       table,
		TableIndex:  tableIndex,
		TotalTables: totalTables,
	})
}

func (h *webProgressHandler) OnTableComplete(table string, upserts, deletes int64) {
	tableIndex, totalTables := h.indexForTable(table)
	h.server.broadcast(ProgressMessage{
		Type:        messageTypeLog,
		Message:     fmt.Sprintf("Completed: %d upserts, %d deletes", upserts, deletes),
		Level:       messageLevelInfo,
		Table:       table,
		TableIndex:  tableIndex,
		TotalTables: totalTables,
	})
}

func (h *webProgressHandler) OnLog(level, message string) {
	// Map slog levels to our levels
	lvl := messageLevelInfo
	switch level {
	case slog.LevelDebug.String():
		lvl = messageLevelDebug
	case slog.LevelWarn.String():
		lvl = messageLevelWarn
	case slog.LevelError.String():
		lvl = messageTypeError
	}

	h.server.broadcast(ProgressMessage{
		Type:    messageTypeLog,
		Message: message,
		Level:   lvl,
	})
}

func (h *webProgressHandler) OnComplete(totalUpserts, totalDeletes int64) {
	progress := float64(100)
	h.server.mu.Lock()
	h.server.syncState.Progress = progress
	h.server.mu.Unlock()

	h.server.broadcast(ProgressMessage{
		Type:     messageTypeComplete,
		Message:  fmt.Sprintf("Sync complete: %d upserts, %d deletes", totalUpserts, totalDeletes),
		Level:    messageLevelInfo,
		Progress: progress,
		Stats: &SyncStats{
			TotalUpserts: totalUpserts,
			TotalDeletes: totalDeletes,
		},
	})
}
