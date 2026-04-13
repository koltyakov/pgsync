package syncer

// ProgressHandler receives progress updates during sync operations
type ProgressHandler interface {
	// OnStart is called when sync begins with the list of tables
	OnStart(tables []string)

	// OnTableStart is called when starting to sync a table
	OnTableStart(table string, index int)

	// OnPartialSync is called when a table will sync only some columns
	OnPartialSync(table string, syncingCols, ignoredCols []string, reason string)

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

func (h *noopProgressHandler) OnStart(_ []string)                              {}
func (h *noopProgressHandler) OnTableStart(_ string, _ int)                    {}
func (h *noopProgressHandler) OnPartialSync(_ string, _, _ []string, _ string) {}
func (h *noopProgressHandler) OnTableComplete(_ string, _, _ int64)            {}
func (h *noopProgressHandler) OnLog(_, _ string)                               {}
func (h *noopProgressHandler) OnComplete(_, _ int64)                           {}
