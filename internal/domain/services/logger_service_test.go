package services_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/hftamayo/gologger/internal/domain/entities"
	"github.com/hftamayo/gologger/internal/domain/services"
	"github.com/hftamayo/gologger/internal/ports"
	"go.uber.org/zap"
)

type fakeStorage struct {
    entries       []entities.LogEntry
    logs          []entities.LogEntry
    serviceNames  []string
    stats         *ports.StorageStats
    storeError    error
    getError      error
    serviceError  error
    statsError    error
    requestedLimit int
}

func (storage *fakeStorage) Store(
    _ context.Context,
    entry entities.LogEntry,
) error {
    if storage.storeError != nil {
        return storage.storeError
    }

    storage.entries = append(storage.entries, entry)
    return nil
}

func (storage *fakeStorage) Get(
    _ context.Context,
    _ string,
    _ entities.LogLevel,
    limit int,
) ([]entities.LogEntry, error) {
    storage.requestedLimit = limit

    if storage.getError != nil {
        return nil, storage.getError
    }

    return storage.logs, nil
}

func (storage *fakeStorage) GetServiceNames(
    context.Context,
) ([]string, error) {
    if storage.serviceError != nil {
        return nil, storage.serviceError
    }

    return storage.serviceNames, nil
}

func (storage *fakeStorage) GetStats(
    context.Context,
    string,
) (*ports.StorageStats, error) {
    if storage.statsError != nil {
        return nil, storage.statsError
    }

    return storage.stats, nil
}

func (*fakeStorage) RotateLogs(context.Context) error {
    return nil
}

func (*fakeStorage) Close() error {
    return nil
}

func newLoggerService(
    storage ports.StorageRepository,
    level entities.LogLevel,
) ports.LoggerService {
    return services.NewLoggerService(
        storage,
        zap.NewNop(),
        &level,
    )
}

func TestLoggerServiceLogEntryStoresEnrichedEntry(t *testing.T) {
    storage := &fakeStorage{}
    service := newLoggerService(storage, entities.LogLevelInfo)

    entry := entities.LogEntry{
        Level:       entities.LogLevelError,
        ServiceName: "orders-service",
        Data: entities.LogData{
            Message: "token=fixture-value",
        },
    }

    if err := service.LogEntry(context.Background(), entry); err != nil {
        t.Fatalf("LogEntry returned error: %v", err)
    }

    if len(storage.entries) != 1 {
        t.Fatalf("expected one stored entry, got %d", len(storage.entries))
    }

    stored := storage.entries[0]

    if stored.ID == "" {
        t.Fatal("expected an ID to be generated")
    }

    if stored.Timestamp.IsZero() {
        t.Fatal("expected timestamp to be generated")
    }

    if stored.EventTimestamp.IsZero() {
        t.Fatal("expected event timestamp to be generated")
    }

    if stored.Data.Message != "[REDACTED]" {
        t.Fatalf(
            "expected redacted message, got %q",
            stored.Data.Message,
        )
    }
}

func TestLoggerServicePreservesProvidedValues(t *testing.T) {
    storage := &fakeStorage{}
    service := newLoggerService(storage, entities.LogLevelInfo)

    timestamp := time.Date(
        2026,
        time.September,
        18,
        12,
        0,
        0,
        0,
        time.UTC,
    )

    entry := entities.LogEntry{
        ID:             "fixture-id",
        Timestamp:      timestamp,
        EventTimestamp: timestamp.Add(-time.Minute),
        Level:          entities.LogLevelInfo,
        ServiceName:    "orders-service",
        Data: entities.LogData{
            Message: "fixture message",
        },
    }

    if err := service.LogEntry(context.Background(), entry); err != nil {
        t.Fatalf("LogEntry returned error: %v", err)
    }

    stored := storage.entries[0]

    if stored.ID != entry.ID {
        t.Fatalf("expected ID %q, got %q", entry.ID, stored.ID)
    }

    if !stored.Timestamp.Equal(entry.Timestamp) {
        t.Fatalf("timestamp was changed")
    }

    if !stored.EventTimestamp.Equal(entry.EventTimestamp) {
        t.Fatalf("event timestamp was changed")
    }
}

func TestLoggerServiceRejectsInvalidEntry(t *testing.T) {
    storage := &fakeStorage{}
    service := newLoggerService(storage, entities.LogLevelInfo)

    entry := entities.LogEntry{
        Level: entities.LogLevelInfo,
        Data: entities.LogData{
            Message: "missing service name",
        },
    }

    err := service.LogEntry(context.Background(), entry)
    if !errors.Is(err, entities.ErrInvalidLogEntry) {
        t.Fatalf("expected invalid entry error, got %v", err)
    }

    if len(storage.entries) != 0 {
        t.Fatalf("expected no stored entries, got %d", len(storage.entries))
    }
}

func TestLoggerServiceFiltersDisabledLevel(t *testing.T) {
    storage := &fakeStorage{}
    service := newLoggerService(storage, entities.LogLevelError)

    entry := entities.LogEntry{
        Level:       entities.LogLevelInfo,
        ServiceName: "orders-service",
        Data: entities.LogData{
            Message: "fixture message",
        },
    }

    if err := service.LogEntry(context.Background(), entry); err != nil {
        t.Fatalf("LogEntry returned error: %v", err)
    }

    if len(storage.entries) != 0 {
        t.Fatalf("expected filtered entry not to be stored")
    }
}

func TestLoggerServiceReturnsStorageError(t *testing.T) {
    expectedError := errors.New("fixture storage error")
    storage := &fakeStorage{storeError: expectedError}
    service := newLoggerService(storage, entities.LogLevelInfo)

    entry := entities.LogEntry{
        Level:       entities.LogLevelInfo,
        ServiceName: "orders-service",
        Data: entities.LogData{
            Message: "fixture message",
        },
    }

    err := service.LogEntry(context.Background(), entry)
    if !errors.Is(err, expectedError) {
        t.Fatalf("expected storage error, got %v", err)
    }
}

func TestLoggerServiceGetLogsUsesDefaultLimit(t *testing.T) {
    storage := &fakeStorage{
        logs: []entities.LogEntry{
            {
                ServiceName: "orders-service",
                Data: entities.LogData{
                    Message: "fixture message",
                },
            },
        },
    }
    service := newLoggerService(storage, entities.LogLevelInfo)

    logs, err := service.GetLogs(
        context.Background(),
        "orders-service",
        entities.LogLevelInfo,
        0,
    )
    if err != nil {
        t.Fatalf("GetLogs returned error: %v", err)
    }

    if len(logs) != 1 {
        t.Fatalf("expected one log, got %d", len(logs))
    }

    if storage.requestedLimit != 100 {
        t.Fatalf(
            "expected default limit 100, got %d",
            storage.requestedLimit,
        )
    }
}

func TestLoggerServiceGetServiceNames(t *testing.T) {
    storage := &fakeStorage{
        serviceNames: []string{"orders-service", "payments-service"},
    }
    service := newLoggerService(storage, entities.LogLevelInfo)

    names, err := service.GetServiceNames(context.Background())
    if err != nil {
        t.Fatalf("GetServiceNames returned error: %v", err)
    }

    if len(names) != 2 {
        t.Fatalf("expected two service names, got %d", len(names))
    }
}

func TestLoggerServiceGetLogStats(t *testing.T) {
    storage := &fakeStorage{
        stats: &ports.StorageStats{
            TotalSize: 42,
        },
    }
    service := newLoggerService(storage, entities.LogLevelInfo)

    stats, err := service.GetLogStats(
        context.Background(),
        "orders-service",
    )
    if err != nil {
        t.Fatalf("GetLogStats returned error: %v", err)
    }

    if stats.TotalEntries != 42 {
        t.Fatalf("expected 42 total entries, got %d", stats.TotalEntries)
    }

    if stats.EntriesByLevel == nil {
        t.Fatal("expected initialized level statistics")
    }

    if stats.EntriesByService == nil {
        t.Fatal("expected initialized service statistics")
    }
}

func TestLoggerServiceStreamLogsClosesOnContextCancel(t *testing.T) {
    storage := &fakeStorage{}
    service := newLoggerService(storage, entities.LogLevelInfo)

    ctx, cancel := context.WithCancel(context.Background())

    stream, err := service.StreamLogs(
        ctx,
        "orders-service",
        entities.LogLevelInfo,
    )
    if err != nil {
        t.Fatalf("StreamLogs returned error: %v", err)
    }

    cancel()

    select {
    case _, open := <-stream:
        if open {
            t.Fatal("expected stream to be closed")
        }
    case <-time.After(time.Second):
        t.Fatal("stream was not closed after context cancellation")
    }
}