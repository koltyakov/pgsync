package sync

// ProgressHandler receives progress updates during sync operations
type ProgressHandler interface {
	// OnStart is called when sync begins with the list of tables
	OnStart(tables []string)

	// OnTableStart is called when starting to sync a table
	OnTableStart(table string, index int)

	// OnTableComplete is called when a table sync finishes
	OnTableComplete(table string, upserts, deletes int64)

	// OnLog receives log messages during sync
	OnLog(level, message string)

	// OnComplete is called when sync finishes
	OnComplete(totalUpserts, totalDeletes int64)
}

// Stats contains sync statistics
type Stats struct {
	TotalUpserts   int64
	TotalDeletes   int64
	UpsertsByTable map[string]int64
	DeletesByTable map[string]int64
	SkippedTables  []string
}

// noopProgressHandler is used when no progress handler is provided
type noopProgressHandler struct{}

func (h *noopProgressHandler) OnStart(tables []string)                              {}
func (h *noopProgressHandler) OnTableStart(table string, index int)                 {}
func (h *noopProgressHandler) OnTableComplete(table string, upserts, deletes int64) {}
func (h *noopProgressHandler) OnLog(level, message string)                          {}
func (h *noopProgressHandler) OnComplete(totalUpserts, totalDeletes int64)          {}
