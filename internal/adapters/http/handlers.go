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
	if expvar.Get("gologger_logs_received") == nil {
		expvar.Publish("gologger_logs_received", expvar.Func(func() any {
			return metrics.receivedLogs.Load()
		}))
	}

	if expvar.Get("gologger_logs_stored") == nil {
		expvar.Publish("gologger_logs_stored", expvar.Func(func() any {
			return metrics.storedLogs.Load()
		}))
	}

	if expvar.Get("gologger_logs_failed") == nil {
		expvar.Publish("gologger_logs_failed", expvar.Func(func() any {
			return metrics.failedLogs.Load()
		}))
	}
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
	if h.metrics != nil {
		h.metrics.receivedLogs.Add(1)
	}

	var req LogEntryRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		if h.metrics != nil {
			h.metrics.failedLogs.Add(1)
		}

		if h.logger != nil {
			h.logger.Error("Failed to decode request body", zap.Error(err))
		}
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	if req.ServiceName == "" {
		http.Error(w, "serviceName is required", http.StatusBadRequest)
		return
	}

	req.Data = security.RedactLogData(req.Data)
	if req.Data.Message == "" {
		http.Error(w, "data.message is required", http.StatusBadRequest)
		return
	}

	entry := entities.LogEntry{
		Timestamp:      time.Now(),
		EventTimestamp: time.Now(),
		Level:          req.Level,
		ServiceName:    req.ServiceName,
		Data:           req.Data,
	}

	if req.EventTimestamp != nil {
		entry.EventTimestamp = *req.EventTimestamp
	}

	ctx := r.Context()
	if err := h.loggerService.LogEntry(ctx, entry); err != nil {
		if h.metrics != nil {
			h.metrics.failedLogs.Add(1)
		}

		if h.logger != nil {
			h.logger.Error("Failed to store log entry", zap.Error(err))
		}
		http.Error(w, internalServerErrorMessage, http.StatusInternalServerError)
		return
	}

	if h.metrics != nil {
		h.metrics.storedLogs.Add(1)
	}

	response := LogEntryResponse{
		ID:        entry.ID,
		Timestamp: entry.Timestamp,
		Status:    "success",
	}

	w.Header().Set(contentTypeHeader, applicationJSON)
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(response)
}

// GetLogs handles GET /logs
func (h *Handler) GetLogs(w http.ResponseWriter, r *http.Request) {
	serviceName := r.URL.Query().Get("serviceName")
	levelStr := r.URL.Query().Get("level")
	limitStr := r.URL.Query().Get("limit")

	var level entities.LogLevel
	if levelStr != "" {
		level = entities.LogLevel(levelStr)
	}

	limit := 100
	if limitStr != "" {
		if l, err := strconv.Atoi(limitStr); err == nil && l > 0 {
			limit = l
		}
	}

	ctx := r.Context()
	logs, err := h.loggerService.GetLogs(ctx, serviceName, level, limit)
	if err != nil {
		if h.logger != nil {
			h.logger.Error("Failed to get logs", zap.Error(err))
		}
		http.Error(w, internalServerErrorMessage, http.StatusInternalServerError)
		return
	}

	w.Header().Set(contentTypeHeader, applicationJSON)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"logs":  logs,
		"count": len(logs),
	})
}

// StreamLogs handles GET /logs/stream (WebSocket)
func (h *Handler) StreamLogs(w http.ResponseWriter, r *http.Request) {
	if h.connectionLimiter != nil {
		if !h.connectionLimiter.TryAcquire() {
			http.Error(w, "connection limit reached", http.StatusServiceUnavailable)
			return
		}
		defer h.connectionLimiter.Release()
	}

	serviceName := r.URL.Query().Get("serviceName")
	levelStr := r.URL.Query().Get("level")

	var level entities.LogLevel
	if levelStr != "" {
		level = entities.LogLevel(levelStr)
	}

	upgrader := websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool {
			return true
		},
	}

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		if h.logger != nil {
			h.logger.Error("Failed to upgrade to WebSocket", zap.Error(err))
		}
		return
	}
	defer conn.Close()

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	stream, err := h.loggerService.StreamLogs(ctx, serviceName, level)
	if err != nil {
		if h.logger != nil {
			h.logger.Error("Failed to start log stream", zap.Error(err))
		}
		_ = conn.WriteJSON(map[string]string{"error": "Failed to start stream"})
		return
	}

	_ = conn.WriteJSON(map[string]string{"status": "connected"})

	for {
		select {
		case entry, ok := <-stream:
			if !ok {
				_ = conn.WriteJSON(map[string]string{"status": "disconnected"})
				return
			}

			if err := conn.WriteJSON(entry); err != nil {
				if h.logger != nil {
					h.logger.Error("Failed to send log entry via WebSocket", zap.Error(err))
				}
				return
			}

		case <-ctx.Done():
			return
		}
	}
}

// GetServices handles GET /services
func (h *Handler) GetServices(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	services, err := h.loggerService.GetServiceNames(ctx)
	if err != nil {
		if h.logger != nil {
			h.logger.Error("Failed to get service names", zap.Error(err))
		}
		http.Error(w, internalServerErrorMessage, http.StatusInternalServerError)
		return
	}

	w.Header().Set(contentTypeHeader, applicationJSON)
	_ = json.NewEncoder(w).Encode(map[string]any{
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
		if h.logger != nil {
			h.logger.Error("Failed to get log stats", zap.Error(err))
		}
		http.Error(w, internalServerErrorMessage, http.StatusInternalServerError)
		return
	}

	w.Header().Set(contentTypeHeader, applicationJSON)
	_ = json.NewEncoder(w).Encode(stats)
}

// HealthCheck handles GET /health
func (h *Handler) HealthCheck(w http.ResponseWriter, r *http.Request) {
	w.Header().Set(contentTypeHeader, applicationJSON)
	_ = json.NewEncoder(w).Encode(map[string]any{
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
