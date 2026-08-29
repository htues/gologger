package ports

import (
	"context"

	"github.com/hftamayo/gologger/internal/domain/entities"
)

// LoggerService defines the primary port for logging operations
type LoggerService interface {
	// LogEntry adds a single log entry to the system
	LogEntry(ctx context.Context, entry entities.LogEntry) error

	// GetLogs retrieves logs with optional filtering
	GetLogs(ctx context.Context, serviceName string, level entities.LogLevel, limit int) ([]entities.LogEntry, error)

	// StreamLogs provides real-time log streaming via WebSocket
	StreamLogs(ctx context.Context, serviceName string, level entities.LogLevel) (<-chan entities.LogEntry, error)

	// GetServiceNames returns all available service names
	GetServiceNames(ctx context.Context) ([]string, error)

	// GetLogStats returns basic statistics about logs
	GetLogStats(ctx context.Context, serviceName string) (*LogStats, error)
}

// LogStats represents basic statistics about logs
type LogStats struct {
	TotalEntries     int64              `json:"totalEntries"`
	EntriesByLevel   map[string]int64   `json:"entriesByLevel"`
	EntriesByService map[string]int64   `json:"entriesByService"`
	LastEntryTime    *entities.LogEntry `json:"lastEntry,omitempty"`
}
