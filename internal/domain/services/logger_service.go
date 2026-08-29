package services

import (
	"context"
	"sync"
	"time"

	"github.com/hftamayo/gologger/internal/domain/entities"
	"github.com/hftamayo/gologger/internal/ports"
	"go.uber.org/zap"
)

// LoggerServiceImpl implements the LoggerService interface
type LoggerServiceImpl struct {
	storage    ports.StorageRepository
	logger     *zap.Logger
	config     *entities.LogLevel
	streams    map[string]chan entities.LogEntry
	streamsMux sync.RWMutex
}

// NewLoggerService creates a new logger service instance
func NewLoggerService(storage ports.StorageRepository, logger *zap.Logger, config *entities.LogLevel) ports.LoggerService {
	return &LoggerServiceImpl{
		storage: storage,
		logger:  logger,
		config:  config,
		streams: make(map[string]chan entities.LogEntry),
	}
}

// LogEntry adds a single log entry to the system
func (ls *LoggerServiceImpl) LogEntry(ctx context.Context, entry entities.LogEntry) error {
	// Validate the log entry
	if !entry.IsValid() {
		ls.logger.Error("Invalid log entry received", zap.String("serviceName", entry.ServiceName))
		return entities.ErrInvalidLogEntry
	}

	// Check if the log level should be processed
	if !entry.IsLevelEnabled(*ls.config) {
		ls.logger.Debug("Log entry filtered by level",
			zap.String("level", entry.GetLogLevel()),
			zap.String("serviceName", entry.ServiceName))
		return nil
	}

	// Set timestamps if not provided
	if entry.Timestamp.IsZero() {
		entry.Timestamp = time.Now()
	}
	if entry.EventTimestamp.IsZero() {
		entry.EventTimestamp = entry.Timestamp
	}

	// Generate ID if not provided
	if entry.ID == "" {
		entry.ID = generateLogID()
	}

	// Store the log entry
	if err := ls.storage.Store(ctx, entry); err != nil {
		ls.logger.Error("Failed to store log entry",
			zap.Error(err),
			zap.String("serviceName", entry.ServiceName))
		return err
	}

	// Broadcast to streams
	ls.broadcastToStreams(entry)

	ls.logger.Debug("Log entry stored successfully",
		zap.String("id", entry.ID),
		zap.String("serviceName", entry.ServiceName),
		zap.String("level", entry.GetLogLevel()))

	return nil
}

// GetLogs retrieves logs with optional filtering
func (ls *LoggerServiceImpl) GetLogs(ctx context.Context, serviceName string, level entities.LogLevel, limit int) ([]entities.LogEntry, error) {
	if limit <= 0 {
		limit = 100 // Default limit
	}

	logs, err := ls.storage.Get(ctx, serviceName, level, limit)
	if err != nil {
		ls.logger.Error("Failed to retrieve logs",
			zap.Error(err),
			zap.String("serviceName", serviceName))
		return nil, err
	}

	ls.logger.Debug("Retrieved logs",
		zap.Int("count", len(logs)),
		zap.String("serviceName", serviceName))

	return logs, nil
}

// StreamLogs provides real-time log streaming via WebSocket
func (ls *LoggerServiceImpl) StreamLogs(ctx context.Context, serviceName string, level entities.LogLevel) (<-chan entities.LogEntry, error) {
	streamKey := serviceName + "_" + string(level)

	ls.streamsMux.Lock()
	defer ls.streamsMux.Unlock()

	// Create new stream channel
	stream := make(chan entities.LogEntry, 100)
	ls.streams[streamKey] = stream

	// Clean up stream when context is cancelled
	go func() {
		<-ctx.Done()
		ls.streamsMux.Lock()
		delete(ls.streams, streamKey)
		ls.streamsMux.Unlock()
		close(stream)
	}()

	ls.logger.Info("Log stream started",
		zap.String("serviceName", serviceName),
		zap.String("level", string(level)))

	return stream, nil
}

// GetServiceNames returns all available service names
func (ls *LoggerServiceImpl) GetServiceNames(ctx context.Context) ([]string, error) {
	services, err := ls.storage.GetServiceNames(ctx)
	if err != nil {
		ls.logger.Error("Failed to get service names", zap.Error(err))
		return nil, err
	}

	return services, nil
}

// GetLogStats returns basic statistics about logs
func (ls *LoggerServiceImpl) GetLogStats(ctx context.Context, serviceName string) (*ports.LogStats, error) {
	stats, err := ls.storage.GetStats(ctx, serviceName)
	if err != nil {
		ls.logger.Error("Failed to get log stats",
			zap.Error(err),
			zap.String("serviceName", serviceName))
		return nil, err
	}

	// Convert storage stats to log stats
	logStats := &ports.LogStats{
		TotalEntries:     stats.TotalSize,
		EntriesByLevel:   make(map[string]int64),
		EntriesByService: make(map[string]int64),
	}

	// For now, we'll return basic stats. In a full implementation,
	// we would aggregate data from the storage layer

	return logStats, nil
}

// broadcastToStreams sends log entries to all active streams
func (ls *LoggerServiceImpl) broadcastToStreams(entry entities.LogEntry) {
	ls.streamsMux.RLock()
	defer ls.streamsMux.RUnlock()

	for key, stream := range ls.streams {
		// Check if this stream should receive this entry
		if shouldSendToStream(key, entry) {
			select {
			case stream <- entry:
				// Successfully sent
			default:
				// Channel is full, skip this entry
				ls.logger.Warn("Stream channel full, dropping log entry",
					zap.String("streamKey", key))
			}
		}
	}
}

// shouldSendToStream determines if a log entry should be sent to a specific stream
func shouldSendToStream(streamKey string, entry entities.LogEntry) bool {
	// Simple implementation - in a full version, we'd parse the streamKey
	// to extract service name and level filters
	return true
}

// generateLogID creates a unique ID for log entries
func generateLogID() string {
	return time.Now().Format("20060102150405") + "_" + randomString(8)
}

// randomString generates a random string of specified length
func randomString(length int) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[time.Now().UnixNano()%int64(len(charset))]
	}
	return string(b)
}
