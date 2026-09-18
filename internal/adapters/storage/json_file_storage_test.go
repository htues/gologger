package storage_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/hftamayo/gologger/internal/adapters/storage"
	"github.com/hftamayo/gologger/internal/contracts"
)

func TestJSONFileStorageStoresAndGetsEvent(t *testing.T) {
    store := newJSONTestStore(t)
    defer store.Close()

    event := jsonTestEvent("event-1")

    if err := store.Store(context.Background(), event); err != nil {
        t.Fatalf("Store returned error: %v", err)
    }

    result, err := store.Get(context.Background(), event.EventID)
    if err != nil {
        t.Fatalf("Get returned error: %v", err)
    }

    if result.EventID != event.EventID {
        t.Fatalf("expected event ID %q, got %q",
            event.EventID,
            result.EventID,
        )
    }

    if result.Message != event.Message {
        t.Fatalf("expected message %q, got %q",
            event.Message,
            result.Message,
        )
    }
}

func TestJSONFileStoragePersistsEventsAcrossReload(t *testing.T) {
    dataDir := t.TempDir()

    store, err := storage.NewJSONFileStorage(
        dataDir,
        1024*1024,
        30,
        nil,
    )
    if err != nil {
        t.Fatalf("NewJSONFileStorage returned error: %v", err)
    }

    event := jsonTestEvent("event-reload")

    if err := store.Store(context.Background(), event); err != nil {
        t.Fatalf("Store returned error: %v", err)
    }

    if err := store.Close(); err != nil {
        t.Fatalf("Close returned error: %v", err)
    }

    reloaded, err := storage.NewJSONFileStorage(
        dataDir,
        1024*1024,
        30,
        nil,
    )
    if err != nil {
        t.Fatalf("reloading storage returned error: %v", err)
    }
    defer reloaded.Close()

    result, err := reloaded.Get(context.Background(), event.EventID)
    if err != nil {
        t.Fatalf("Get after reload returned error: %v", err)
    }

    if result.EventID != event.EventID {
        t.Fatalf("expected event ID %q, got %q",
            event.EventID,
            result.EventID,
        )
    }
}

func TestJSONFileStorageRejectsDuplicateEvent(t *testing.T) {
    store := newJSONTestStore(t)
    defer store.Close()

    event := jsonTestEvent("event-duplicate")

    if err := store.Store(context.Background(), event); err != nil {
        t.Fatalf("first Store returned error: %v", err)
    }

    err := store.Store(context.Background(), event)
    if !errors.Is(err, storage.ErrDuplicateEvent) {
        t.Fatalf("expected duplicate event error, got %v", err)
    }
}

func TestJSONFileStorageRejectsMissingEventID(t *testing.T) {
    store := newJSONTestStore(t)
    defer store.Close()

    event := jsonTestEvent("")

    err := store.Store(context.Background(), event)
    if err == nil {
        t.Fatal("expected missing event ID error")
    }
}

func TestJSONFileStorageQueriesWithFilters(t *testing.T) {
    store := newJSONTestStore(t)
    defer store.Close()

    events := []contracts.Event{
        {
            EventID:   "event-info",
            Timestamp: time.Now().UTC().Add(-2 * time.Minute),
            Level:     contracts.EventLevelInfo,
            Service:   "orders",
            EventType: "order_created",
            Message:   "Order created",
        },
        {
            EventID:   "event-error",
            Timestamp: time.Now().UTC().Add(-1 * time.Minute),
            Level:     contracts.EventLevelError,
            Service:   "orders",
            EventType: "order_failed",
            Message:   "Order failed",
        },
        {
            EventID:   "event-other-service",
            Timestamp: time.Now().UTC(),
            Level:     contracts.EventLevelInfo,
            Service:   "payments",
            EventType: "payment_created",
            Message:   "Payment created",
        },
    }

    for _, event := range events {
        if err := store.Store(context.Background(), event); err != nil {
            t.Fatalf("Store returned error: %v", err)
        }
    }

    result, err := store.Query(context.Background(), contracts.EventFilter{
        Services: []string{"orders"},
        Levels:   []contracts.EventLevel{contracts.EventLevelInfo},
    })
    if err != nil {
        t.Fatalf("Query returned error: %v", err)
    }

    if len(result) != 1 {
        t.Fatalf("expected one matching event, got %d", len(result))
    }

    if result[0].EventID != "event-info" {
        t.Fatalf("expected event-info, got %q", result[0].EventID)
    }
}

func TestJSONFileStorageRotatesWhenFileSizeIsExceeded(t *testing.T) {
    dataDir := t.TempDir()

    store, err := storage.NewJSONFileStorage(
        dataDir,
        1,
        30,
        nil,
    )
    if err != nil {
        t.Fatalf("NewJSONFileStorage returned error: %v", err)
    }
    defer store.Close()

    for index := 0; index < 2; index++ {
        event := jsonTestEvent("event-size-" + string(rune('a'+index)))

        if err := store.Store(context.Background(), event); err != nil {
            t.Fatalf("Store returned error: %v", err)
        }
    }

    files, err := filepath.Glob(
        filepath.Join(dataDir, "events_*.jsonl"),
    )
    if err != nil {
        t.Fatalf("Glob returned error: %v", err)
    }

    if len(files) < 2 {
        t.Fatalf("expected at least two rotated files, got %d", len(files))
    }
}

func TestJSONFileStorageRotatesExpiredFiles(t *testing.T) {
    dataDir := t.TempDir()

    store, err := storage.NewJSONFileStorage(
        dataDir,
        1024*1024,
        1,
        nil,
    )
    if err != nil {
        t.Fatalf("NewJSONFileStorage returned error: %v", err)
    }
    defer store.Close()

    expiredFile := filepath.Join(
        dataDir,
        "events_20000103_00.jsonl",
    )

    eventBytes, err := json.Marshal(jsonTestEvent("expired-event"))
    if err != nil {
        t.Fatalf("Marshal returned error: %v", err)
    }

    if err := os.WriteFile(
        expiredFile,
        append(eventBytes, '\n'),
        0600,
    ); err != nil {
        t.Fatalf("WriteFile returned error: %v", err)
    }

    expiredTime := time.Now().Add(-48 * time.Hour)
    if err := os.Chtimes(expiredFile, expiredTime, expiredTime); err != nil {
        t.Fatalf("Chtimes returned error: %v", err)
    }

    if err := store.Rotate(context.Background()); err != nil {
        t.Fatalf("Rotate returned error: %v", err)
    }

    if _, err := os.Stat(expiredFile); !errors.Is(err, os.ErrNotExist) {
        t.Fatalf("expected expired file to be removed, got %v", err)
    }
}

func TestJSONFileStorageRejectsCanceledContext(t *testing.T) {
    store := newJSONTestStore(t)
    defer store.Close()

    ctx, cancel := context.WithCancel(context.Background())
    cancel()

    event := jsonTestEvent("event-canceled")

    if err := store.Store(ctx, event); !errors.Is(err, context.Canceled) {
        t.Fatalf("expected context canceled error, got %v", err)
    }

    if _, err := store.Get(ctx, event.EventID); !errors.Is(
        err,
        context.Canceled,
    ) {
        t.Fatalf("expected context canceled error from Get, got %v", err)
    }

    if _, err := store.Query(ctx, contracts.EventFilter{}); !errors.Is(
        err,
        context.Canceled,
    ) {
        t.Fatalf("expected context canceled error from Query, got %v", err)
    }
}

func TestJSONFileStorageRejectsOperationsAfterClose(t *testing.T) {
    store := newJSONTestStore(t)

    if err := store.Close(); err != nil {
        t.Fatalf("Close returned error: %v", err)
    }

    event := jsonTestEvent("event-closed")

    if err := store.Store(context.Background(), event); !errors.Is(
        err,
        storage.ErrStoreClosed,
    ) {
        t.Fatalf("expected closed store error from Store, got %v", err)
    }

    if err := store.Health(context.Background()); !errors.Is(
        err,
        storage.ErrStoreClosed,
    ) {
        t.Fatalf("expected closed store error from Health, got %v", err)
    }

    if err := store.Rotate(context.Background()); !errors.Is(
        err,
        storage.ErrStoreClosed,
    ) {
        t.Fatalf("expected closed store error from Rotate, got %v", err)
    }
}

func TestJSONFileStorageHealth(t *testing.T) {
    store := newJSONTestStore(t)
    defer store.Close()

    if err := store.Health(context.Background()); err != nil {
        t.Fatalf("Health returned error: %v", err)
    }
}

func newJSONTestStore(t *testing.T) *storage.JSONFileStorage {
    t.Helper()

    store, err := storage.NewJSONFileStorage(
        t.TempDir(),
        1024*1024,
        30,
        nil,
    )
    if err != nil {
        t.Fatalf("NewJSONFileStorage returned error: %v", err)
    }

    return store
}

func jsonTestEvent(eventID string) contracts.Event {
    return contracts.Event{
        EventID:   eventID,
        Timestamp: time.Now().UTC(),
        Level:     contracts.EventLevelInfo,
        Service:   "test-service",
        EventType: "test-event",
        Message:   "fixture message",
    }
}