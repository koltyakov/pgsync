package server

import (
	"context"
	"fmt"
	"io/fs"
	"log/slog"
	"net/http"
	"regexp"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	webui "github.com/koltyakov/pgsync/web"
)

// Server provides a web UI for pgsync
type Server struct {
	port     int
	sourceDB string
	targetDB string
	schema   string

	mu        sync.Mutex
	syncState *SyncState
	clients   map[*websocket.Conn]bool
	upgrader  websocket.Upgrader
	logger    *slog.Logger
}

// SyncState tracks current sync operation state
type SyncState struct {
	Running     bool      `json:"running"`
	StartedAt   time.Time `json:"startedAt,omitempty"`
	CurrentStep string    `json:"currentStep,omitempty"`
	Progress    float64   `json:"progress"` // 0-100
	Tables      []string  `json:"tables,omitempty"`
	TableIndex  int       `json:"tableIndex"`
}

// ProgressMessage is sent via WebSocket to update clients
type ProgressMessage struct {
	Type        string     `json:"type"` // "progress", "log", "complete", "error"
	Message     string     `json:"message,omitempty"`
	Level       string     `json:"level,omitempty"` // "info", "debug", "error", "warn"
	Progress    float64    `json:"progress,omitempty"`
	Table       string     `json:"table,omitempty"`
	TableIndex  int        `json:"tableIndex,omitempty"`
	TotalTables int        `json:"totalTables,omitempty"`
	Stats       *SyncStats `json:"stats,omitempty"`
	Timestamp   time.Time  `json:"timestamp"`
}

// SyncStats contains sync statistics
type SyncStats struct {
	TotalUpserts int64            `json:"totalUpserts"`
	TotalDeletes int64            `json:"totalDeletes"`
	TableStats   map[string]int64 `json:"tableStats,omitempty"`
}

// Config holds server configuration
type Config struct {
	Port     int
	SourceDB string
	TargetDB string
	Schema   string
}

// New creates a new Server instance
func New(cfg *Config) *Server {
	return &Server{
		port:     cfg.Port,
		sourceDB: cfg.SourceDB,
		targetDB: cfg.TargetDB,
		schema:   cfg.Schema,
		clients:  make(map[*websocket.Conn]bool),
		upgrader: websocket.Upgrader{
			CheckOrigin: func(r *http.Request) bool {
				return true // Allow all origins for local dev
			},
		},
		syncState: &SyncState{},
		logger:    slog.Default(),
	}
}

// Start runs the HTTP server
func (s *Server) Start(ctx context.Context) error {
	mux := http.NewServeMux()

	// API routes
	mux.HandleFunc("/api/config", s.handleGetConfig)
	mux.HandleFunc("/api/schema/tables", s.handleGetTables)
	mux.HandleFunc("/api/schema/table/", s.handleGetTableInfo)
	mux.HandleFunc("/api/sync/start", s.handleStartSync)
	mux.HandleFunc("/api/sync/status", s.handleSyncStatus)
	mux.HandleFunc("/ws", s.handleWebSocket)

	// Serve embedded frontend or fallback message
	webDist, err := webui.GetWebFS()
	if err != nil {
		s.logger.Warn("No embedded frontend found, serving API only", "error", err)
		mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/html")
			fmt.Fprint(w, `<!DOCTYPE html><html><head><title>pgsync</title></head><body>
				<h1>pgsync Web UI</h1>
				<p>Frontend not built. Build with:</p>
				<pre>make web</pre>
			</body></html>`)
		})
	} else {
		fileServer := http.FileServer(http.FS(webDist))
		mux.Handle("/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Try to serve the file, fallback to index.html for SPA routing
			path := r.URL.Path
			if path == "/" {
				path = "/index.html"
			}
			if _, err := fs.Stat(webDist, path[1:]); err != nil {
				// File not found, serve index.html for SPA
				r.URL.Path = "/"
			}
			fileServer.ServeHTTP(w, r)
		}))
	}

	addr := fmt.Sprintf(":%d", s.port)
	server := &http.Server{
		Addr:    addr,
		Handler: corsMiddleware(mux),
	}

	s.logger.Info("Starting pgsync web server", "port", s.port, "url", fmt.Sprintf("http://localhost:%d", s.port))

	// Graceful shutdown
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()

	if err := server.ListenAndServe(); err != http.ErrServerClosed {
		return fmt.Errorf("server error: %w", err)
	}
	return nil
}

// corsMiddleware adds CORS headers for development
func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")

		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}

		next.ServeHTTP(w, r)
	})
}

// broadcast sends a message to all connected WebSocket clients
func (s *Server) broadcast(msg ProgressMessage) {
	msg.Timestamp = time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()

	for client := range s.clients {
		if err := client.WriteJSON(msg); err != nil {
			s.logger.Debug("Failed to write to websocket client", "error", err)
			client.Close()
			delete(s.clients, client)
		}
	}
}

// ConfigResponse contains server configuration for the UI
type ConfigResponse struct {
	SourceDB string `json:"sourceDb"`
	TargetDB string `json:"targetDb"`
	Schema   string `json:"schema"`
}

// maskPassword replaces password in connection string with ***
func maskPassword(connStr string) string {
	// Match patterns like :password@ or password=xxx
	re := regexp.MustCompile(`(:)([^:@]+)(@)`)
	masked := re.ReplaceAllString(connStr, "$1***$3")

	// Also handle password=xxx format
	re2 := regexp.MustCompile(`(password=)([^&\s]+)`)
	masked = re2.ReplaceAllString(masked, "$1***")

	return masked
}

// handleGetConfig returns server configuration with masked passwords
func (s *Server) handleGetConfig(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	s.writeJSON(w, ConfigResponse{
		SourceDB: maskPassword(s.sourceDB),
		TargetDB: maskPassword(s.targetDB),
		Schema:   s.schema,
	})
}

// handleWebSocket manages WebSocket connections
func (s *Server) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		s.logger.Error("WebSocket upgrade failed", "error", err)
		return
	}

	s.mu.Lock()
	s.clients[conn] = true
	s.mu.Unlock()

	s.logger.Debug("WebSocket client connected", "addr", conn.RemoteAddr())

	// Send current state on connect
	s.mu.Lock()
	state := *s.syncState
	s.mu.Unlock()

	conn.WriteJSON(ProgressMessage{
		Type:     "status",
		Progress: state.Progress,
		Message:  state.CurrentStep,
	})

	// Keep connection alive and handle disconnect
	defer func() {
		s.mu.Lock()
		delete(s.clients, conn)
		s.mu.Unlock()
		conn.Close()
	}()

	for {
		_, _, err := conn.ReadMessage()
		if err != nil {
			break
		}
	}
}
