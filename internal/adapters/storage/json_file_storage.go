package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/hftamayo/gologgermservice/internal/domain/entities"
	"github.com/hftamayo/gologgermservice/internal/ports"
	"go.uber.org/zap"
)

// JSONFileStorage implements the StorageRepository interface using JSON files
type JSONFileStorage struct {
	dataDir      string
	rotationDays int
	maxFileSize  int64
	bufferSize   int
	logger       *zap.Logger
	mutex        sync.RWMutex
	buffer       []entities.LogEntry
	bufferMutex  sync.Mutex
}

// NewJSONFileStorage creates a new JSON file storage instance
func NewJSONFileStorage(dataDir string, rotationDays int, maxFileSize int64, bufferSize int, logger *zap.Logger) (*JSONFileStorage, error) {
	// Create data directory if it doesn't exist
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data directory: %w", err)
	}

	storage := &JSONFileStorage{
		dataDir:      dataDir,
		rotationDays: rotationDays,
		maxFileSize:  maxFileSize,
		bufferSize:   bufferSize,
		logger:       logger,
		buffer:       make([]entities.LogEntry, 0, bufferSize),
	}

	// Start background tasks
	go storage.startBackgroundTasks()

	return storage, nil
}

// Store stores a log entry
func (jfs *JSONFileStorage) Store(ctx context.Context, entry entities.LogEntry) error {
	jfs.bufferMutex.Lock()
	defer jfs.bufferMutex.Unlock()

	// Add to buffer
	jfs.buffer = append(jfs.buffer, entry)

	// Flush buffer if it's full
	if len(jfs.buffer) >= jfs.bufferSize {
		return jfs.flushBuffer()
	}

	return nil
}

// Get retrieves log entries with filtering
func (jfs *JSONFileStorage) Get(ctx context.Context, serviceName string, level entities.LogLevel, limit int) ([]entities.LogEntry, error) {
	jfs.mutex.RLock()
	defer jfs.mutex.RUnlock()

	// Flush buffer first to ensure we have latest data
	jfs.bufferMutex.Lock()
	if len(jfs.buffer) > 0 {
		if err := jfs.flushBuffer(); err != nil {
			jfs.bufferMutex.Unlock()
			return nil, err
		}
	}
	jfs.bufferMutex.Unlock()

	var allEntries []entities.LogEntry

	// Read from all files for the service
	files, err := jfs.getServiceFiles(serviceName)
	if err != nil {
		return nil, err
	}

	for _, file := range files {
		entries, err := jfs.readFile(file)
		if err != nil {
			jfs.logger.Warn("Failed to read file", zap.String("file", file), zap.Error(err))
			continue
		}

		// Filter entries
		for _, entry := range entries {
			if (serviceName == "" || entry.ServiceName == serviceName) &&
				(level == "" || entry.Level == level) {
				allEntries = append(allEntries, entry)
			}
		}
	}

	// Sort by timestamp (newest first)
	sort.Slice(allEntries, func(i, j int) bool {
		return allEntries[i].Timestamp.After(allEntries[j].Timestamp)
	})

	// Apply limit
	if limit > 0 && len(allEntries) > limit {
		allEntries = allEntries[:limit]
	}

	return allEntries, nil
}

// GetServiceNames returns all available service names
func (jfs *JSONFileStorage) GetServiceNames(ctx context.Context) ([]string, error) {
	jfs.mutex.RLock()
	defer jfs.mutex.RUnlock()

	services := make(map[string]bool)

	// Read all files in data directory
	err := filepath.WalkDir(jfs.dataDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if !d.IsDir() && strings.HasSuffix(path, ".json") {
			// Extract service name from filename
			filename := filepath.Base(path)
			serviceName := strings.TrimSuffix(filename, filepath.Ext(filename))
			// Remove date suffix
			if idx := strings.LastIndex(serviceName, "_"); idx != -1 {
				serviceName = serviceName[:idx]
			}
			services[serviceName] = true
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	// Convert map to slice
	var result []string
	for service := range services {
		result = append(result, service)
	}

	sort.Strings(result)
	return result, nil
}

// GetStats returns storage statistics
func (jfs *JSONFileStorage) GetStats(ctx context.Context, serviceName string) (*ports.StorageStats, error) {
	jfs.mutex.RLock()
	defer jfs.mutex.RUnlock()

	stats := &ports.StorageStats{
		TotalFiles: 0,
		TotalSize:  0,
	}

	files, err := jfs.getServiceFiles(serviceName)
	if err != nil {
		return nil, err
	}

	for _, file := range files {
		fileInfo, err := os.Stat(file)
		if err != nil {
			continue
		}

		stats.TotalFiles++
		stats.TotalSize += fileInfo.Size()

		if stats.OldestFile == "" || fileInfo.ModTime().Before(time.Now()) {
			stats.OldestFile = filepath.Base(file)
		}
		if stats.NewestFile == "" || fileInfo.ModTime().After(time.Now()) {
			stats.NewestFile = filepath.Base(file)
		}
	}

	return stats, nil
}

// RotateLogs performs log rotation if needed
func (jfs *JSONFileStorage) RotateLogs(ctx context.Context) error {
	jfs.mutex.Lock()
	defer jfs.mutex.Unlock()

	// Flush buffer first
	jfs.bufferMutex.Lock()
	if len(jfs.buffer) > 0 {
		if err := jfs.flushBuffer(); err != nil {
			jfs.bufferMutex.Unlock()
			return err
		}
	}
	jfs.bufferMutex.Unlock()

	// Check for old files and remove them
	cutoffDate := time.Now().AddDate(0, 0, -jfs.rotationDays)

	err := filepath.WalkDir(jfs.dataDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if !d.IsDir() && strings.HasSuffix(path, ".json") {
			fileInfo, err := d.Info()
			if err != nil {
				return err
			}

			if fileInfo.ModTime().Before(cutoffDate) {
				if err := os.Remove(path); err != nil {
					jfs.logger.Warn("Failed to remove old log file", zap.String("file", path), zap.Error(err))
				} else {
					jfs.logger.Info("Removed old log file", zap.String("file", path))
				}
			}
		}

		return nil
	})

	return err
}

// Close closes the storage connection
func (jfs *JSONFileStorage) Close() error {
	jfs.bufferMutex.Lock()
	defer jfs.bufferMutex.Unlock()

	// Flush remaining buffer
	if len(jfs.buffer) > 0 {
		return jfs.flushBuffer()
	}

	return nil
}

// flushBuffer writes buffered entries to file
func (jfs *JSONFileStorage) flushBuffer() error {
	if len(jfs.buffer) == 0 {
		return nil
	}

	// Group entries by service name
	serviceGroups := make(map[string][]entities.LogEntry)
	for _, entry := range jfs.buffer {
		serviceGroups[entry.ServiceName] = append(serviceGroups[entry.ServiceName], entry)
	}

	// Write each service group to its file
	for serviceName, entries := range serviceGroups {
		filename := jfs.getFilename(serviceName)
		
		// Read existing entries
		existingEntries, err := jfs.readFile(filename)
		if err != nil && !os.IsNotExist(err) {
			return err
		}

		// Append new entries
		allEntries := append(existingEntries, entries...)

		// Write back to file
		if err := jfs.writeFile(filename, allEntries); err != nil {
			return err
		}
	}

	// Clear buffer
	jfs.buffer = jfs.buffer[:0]

	return nil
}

// getFilename generates filename for a service
func (jfs *JSONFileStorage) getFilename(serviceName string) string {
	// Get end of week date (Sunday)
	now := time.Now()
	daysUntilSunday := int(time.Sunday - now.Weekday())
	if daysUntilSunday == 0 {
		daysUntilSunday = 7
	}
	endOfWeek := now.AddDate(0, 0, daysUntilSunday)
	
	dateStr := endOfWeek.Format("010206") // MM/DD/YY format
	return filepath.Join(jfs.dataDir, fmt.Sprintf("%s_%s.json", serviceName, dateStr))
}

// getServiceFiles returns all files for a service
func (jfs *JSONFileStorage) getServiceFiles(serviceName string) ([]string, error) {
	var files []string

	err := filepath.WalkDir(jfs.dataDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if !d.IsDir() && strings.HasSuffix(path, ".json") {
			filename := filepath.Base(path)
			if strings.HasPrefix(filename, serviceName+"_") {
				files = append(files, path)
			}
		}

		return nil
	})

	return files, err
}

// readFile reads entries from a JSON file
func (jfs *JSONFileStorage) readFile(filename string) ([]entities.LogEntry, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, err
	}

	if len(data) == 0 {
		return []entities.LogEntry{}, nil
	}

	var entries []entities.LogEntry
	if err := json.Unmarshal(data, &entries); err != nil {
		return nil, err
	}

	return entries, nil
}

// writeFile writes entries to a JSON file
func (jfs *JSONFileStorage) writeFile(filename string, entries []entities.LogEntry) error {
	data, err := json.MarshalIndent(entries, "", "  ")
	if err != nil {
		return err
	}

	// Create directory if it doesn't exist
	dir := filepath.Dir(filename)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	return os.WriteFile(filename, data, 0644)
}

// startBackgroundTasks starts background tasks like rotation
func (jfs *JSONFileStorage) startBackgroundTasks() {
	ticker := time.NewTicker(1 * time.Hour)
	defer ticker.Stop()

	for range ticker.C {
		if err := jfs.RotateLogs(context.Background()); err != nil {
			jfs.logger.Error("Failed to rotate logs", zap.Error(err))
		}
	}
} 