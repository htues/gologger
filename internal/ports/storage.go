package ports

import (
	"context"

	"github.com/hftamayo/gologger/internal/domain/entities"
)

// StorageRepository defines the secondary port for data persistence
type StorageRepository interface {
	// Store stores a log entry
	Store(ctx context.Context, entry entities.LogEntry) error

	// Get retrieves log entries with filtering
	Get(ctx context.Context, serviceName string, level entities.LogLevel, limit int) ([]entities.LogEntry, error)

	// GetServiceNames returns all available service names
	GetServiceNames(ctx context.Context) ([]string, error)

	// GetStats returns storage statistics
	GetStats(ctx context.Context, serviceName string) (*StorageStats, error)

	// RotateLogs performs log rotation if needed
	RotateLogs(ctx context.Context) error

	// Close closes the storage connection
	Close() error
}

// StorageStats represents storage statistics
type StorageStats struct {
	TotalFiles int64  `json:"totalFiles"`
	TotalSize  int64  `json:"totalSize"`
	OldestFile string `json:"oldestFile"`
	NewestFile string `json:"newestFile"`
}
