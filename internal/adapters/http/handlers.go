package http

import (
	"context"
	"encoding/json"
	"expvar"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/gorilla/mux"
	"github.com/gorilla/websocket"
	"github.com/hftamayo/gologger/internal/domain/entities"
	"github.com/hftamayo/gologger/internal/ports"
	"github.com/hftamayo/gologger/internal/security"
	"go.uber.org/zap"
)

const (
	internalServerErrorMessage = "Internal server error"
	contentTypeHeader          = "Content-Type"
	applicationJSON            = "application/json"
)

type Metrics struct {
	receivedLogs atomic.Int64
	storedLogs   atomic.Int64
	failedLogs   atomic.Int64
}

// Handler handles HTTP requests for the logger service
type Handler struct {
	loggerService     ports.LoggerService
	store             ports.EventStore
	logger            *zap.Logger
	metrics           *Metrics
	connectionLimiter *security.ConnectionLimiter
}

func newMetrics() *Metrics {
	return &Metrics{}
}

func (metrics *Metrics) publish() {
	expvar.Publish("gologger_logs_received", expvar.Func(func() any {
		return metrics.receivedLogs.Load()
	}))

	expvar.Publish("gologger_logs_stored", expvar.Func(func() any {
		return metrics.storedLogs.Load()
	}))

	expvar.Publish("gologger_logs_failed", expvar.Func(func() any {
		return metrics.failedLogs.Load()
	}))
}

// NewHandler creates a new HTTP handler
func NewHandler(
	loggerService ports.LoggerService,
	store ports.EventStore,
	logger *zap.Logger,
) *Handler {
	metrics := newMetrics()
	metrics.publish()

	return &Handler{
		loggerService: loggerService,
		store:         store,
		logger:        logger,
		metrics:       metrics,
	}
}

func (h *Handler) SetConnectionLimiter(limiter *security.ConnectionLimiter) {
	h.connectionLimiter = limiter
}

// LogEntryRequest represents the request body for logging an entry
type LogEntryRequest struct {
	EventTimestamp *time.Time        `json:"event_timestamp,omitempty"`
	Level          entities.LogLevel `json:"level"`
	ServiceName    string            `json:"serviceName"`
	Data           entities.LogData  `json:"data"`
}

// LogEntryResponse represents the response for a log entry
type LogEntryResponse struct {
	ID        string    `json:"id"`
	Timestamp time.Time `json:"timestamp"`
	Status    string    `json:"status"`
}

// GetLogsRequest represents the request parameters for getting logs
type GetLogsRequest struct {
	ServiceName string            `json:"serviceName,omitempty"`
	Level       entities.LogLevel `json:"level,omitempty"`
	Limit       int               `json:"limit,omitempty"`
}

// RegisterRoutes registers all HTTP routes
func (h *Handler) RegisterRoutes(router *mux.Router) {
	router.HandleFunc("/logs", h.LogEntry).Methods("POST")
	router.HandleFunc("/logs", h.GetLogs).Methods("GET")
	router.HandleFunc("/logs/stream", h.StreamLogs).Methods("GET")
	router.HandleFunc("/services", h.GetServices).Methods("GET")
	router.HandleFunc("/stats", h.GetStats).Methods("GET")

	router.HandleFunc("/health", h.HealthCheck).Methods("GET")
	router.HandleFunc("/health/live", h.LivenessCheck).Methods("GET")
	router.HandleFunc("/health/ready", h.ReadinessCheck).Methods("GET")
	router.HandleFunc("/metrics", h.Metrics).Methods("GET")
}

// LogEntry handles POST /logs
func (h *Handler) LogEntry(w http.ResponseWriter, r *http.Request) {
	h.metrics.receivedLogs.Add(1)

	var req LogEntryRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.metrics.failedLogs.Add(1)

		h.logger.Error("Failed to decode request body", zap.Error(err))
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	// Validate required fields
	if req.ServiceName == "" {
		http.Error(w, "serviceName is required", http.StatusBadRequest)
		return
	}
	req.Data = security.RedactLogData(req.Data)
	if req.Data.Message == "" {
		http.Error(w, "data.message is required", http.StatusBadRequest)
		return
	}

	// Create log entry
	entry := entities.LogEntry{
		Timestamp:      time.Now(),
		EventTimestamp: time.Now(),
		Level:          req.Level,
		ServiceName:    req.ServiceName,
		Data:           req.Data,
	}

	// Use event timestamp if provided
	if req.EventTimestamp != nil {
		entry.EventTimestamp = *req.EventTimestamp
	}

	// Store the log entry
	ctx := r.Context()
	if err := h.loggerService.LogEntry(ctx, entry); err != nil {
		h.metrics.failedLogs.Add(1)
		h.logger.Error("Failed to store log entry", zap.Error(err))
		http.Error(w, internalServerErrorMessage, http.StatusInternalServerError)
		return
	}
	h.metrics.storedLogs.Add(1)

	// Return response
	response := LogEntryResponse{
		ID:        entry.ID,
		Timestamp: entry.Timestamp,
		Status:    "success",
	}

	w.Header().Set(contentTypeHeader, applicationJSON)
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(response)
}

// GetLogs handles GET /logs
func (h *Handler) GetLogs(w http.ResponseWriter, r *http.Request) {
	// Parse query parameters
	serviceName := r.URL.Query().Get("serviceName")
	levelStr := r.URL.Query().Get("level")
	limitStr := r.URL.Query().Get("limit")

	var level entities.LogLevel
	if levelStr != "" {
		level = entities.LogLevel(levelStr)
	}

	limit := 100 // Default limit
	if limitStr != "" {
		if l, err := strconv.Atoi(limitStr); err == nil && l > 0 {
			limit = l
		}
	}

	// Get logs
	ctx := r.Context()
	logs, err := h.loggerService.GetLogs(ctx, serviceName, level, limit)
	if err != nil {
		h.logger.Error("Failed to get logs", zap.Error(err))
		http.Error(w, internalServerErrorMessage, http.StatusInternalServerError)
		return
	}

	// Return response
	w.Header().Set(contentTypeHeader, applicationJSON)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"logs":  logs,
		"count": len(logs),
	})
}

// StreamLogs handles GET /logs/stream (WebSocket)
func (h *Handler) StreamLogs(w http.ResponseWriter, r *http.Request) {
	if !h.connectionLimiter.TryAcquire() {
		http.Error(w, "connection limit reached", http.StatusServiceUnavailable)
		return
	}
	defer h.connectionLimiter.Release()
	// Parse query parameters
	serviceName := r.URL.Query().Get("serviceName")
	levelStr := r.URL.Query().Get("level")

	var level entities.LogLevel
	if levelStr != "" {
		level = entities.LogLevel(levelStr)
	}

	// Upgrade to WebSocket
	upgrader := websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool {
			return true // Allow all origins for now
		},
	}

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		h.logger.Error("Failed to upgrade to WebSocket", zap.Error(err))
		return
	}
	defer conn.Close()

	// Create context for the stream
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	// Start log stream
	stream, err := h.loggerService.StreamLogs(ctx, serviceName, level)
	if err != nil {
		h.logger.Error("Failed to start log stream", zap.Error(err))
		conn.WriteJSON(map[string]string{"error": "Failed to start stream"})
		return
	}

	// Send initial message
	conn.WriteJSON(map[string]string{"status": "connected"})

	// Stream logs
	for {
		select {
		case entry, ok := <-stream:
			if !ok {
				// Stream closed
				conn.WriteJSON(map[string]string{"status": "disconnected"})
				return
			}

			// Send log entry
			if err := conn.WriteJSON(entry); err != nil {
				h.logger.Error("Failed to send log entry via WebSocket", zap.Error(err))
				return
			}

		case <-ctx.Done():
			// Context cancelled
			return
		}
	}
}

// GetServices handles GET /services
func (h *Handler) GetServices(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	services, err := h.loggerService.GetServiceNames(ctx)
	if err != nil {
		h.logger.Error("Failed to get service names", zap.Error(err))
		http.Error(w, internalServerErrorMessage, http.StatusInternalServerError)
		return
	}

	w.Header().Set(contentTypeHeader, applicationJSON)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"services": services,
		"count":    len(services),
	})
}

// GetStats handles GET /stats
func (h *Handler) GetStats(w http.ResponseWriter, r *http.Request) {
	serviceName := r.URL.Query().Get("serviceName")

	ctx := r.Context()
	stats, err := h.loggerService.GetLogStats(ctx, serviceName)
	if err != nil {
		h.logger.Error("Failed to get log stats", zap.Error(err))
		http.Error(w, internalServerErrorMessage, http.StatusInternalServerError)
		return
	}

	w.Header().Set(contentTypeHeader, applicationJSON)
	json.NewEncoder(w).Encode(stats)
}

// HealthCheck handles GET /health
func (h *Handler) HealthCheck(w http.ResponseWriter, r *http.Request) {
	w.Header().Set(contentTypeHeader, applicationJSON)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":    "healthy",
		"timestamp": time.Now(),
		"service":   "logger-service",
	})
}

// LivenessCheck handles GET /health/live
func (h *Handler) LivenessCheck(w http.ResponseWriter, r *http.Request) {
	h.writeJSON(w, http.StatusOK, map[string]any{
		"status":    "alive",
		"timestamp": time.Now().UTC(),
	})
}

func (h *Handler) ReadinessCheck(w http.ResponseWriter, r *http.Request) {
	if err := h.store.Health(r.Context()); err != nil {
		h.writeJSON(w, http.StatusServiceUnavailable, map[string]any{
			"status": "not_ready",
			"error":  "storage unavailable",
		})
		return
	}

	h.writeJSON(w, http.StatusOK, map[string]any{
		"status":    "ready",
		"timestamp": time.Now().UTC(),
	})
}

func (h *Handler) HealthCheck(w http.ResponseWriter, r *http.Request) {
	h.LivenessCheck(w, r)
}

func (h *Handler) Metrics(w http.ResponseWriter, r *http.Request) {
	expvar.Handler().ServeHTTP(w, r)
}

func (h *Handler) writeJSON(
	w http.ResponseWriter,
	status int,
	value any,
) {
	w.Header().Set(contentTypeHeader, applicationJSON)
	w.WriteHeader(status)

	_ = json.NewEncoder(w).Encode(value)
}
