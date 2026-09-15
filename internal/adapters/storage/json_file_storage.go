package storage

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/hftamayo/gologger/internal/contracts"
	"go.uber.org/zap"
)

var (
    ErrEventNotFound = errors.New("event not found")
    ErrStoreClosed   = errors.New("event store is closed")
    ErrDuplicateEvent = errors.New("event already exists")
)

type JSONFileStorage struct {
    mu sync.RWMutex

    dataDir       string
    maxFileSize   int64
    retentionDays int
    logger        *zap.Logger
    knownIDs      map[string]struct{}
    closed        bool
}

// NewJSONFileStorage creates a JSON Lines event store.
//
// Events are written to weekly files. When maxFileSize is exceeded,
// another numbered file is created for the same week.
func NewJSONFileStorage(
    dataDir string,
    maxFileSize int64,
    retentionDays int,
    logger *zap.Logger,
) (*JSONFileStorage, error) {
    if strings.TrimSpace(dataDir) == "" {
        return nil, errors.New("data directory is required")
    }

    if maxFileSize <= 0 {
        maxFileSize = 10 * 1024 * 1024
    }

    if retentionDays <= 0 {
        retentionDays = 30
    }

    if logger == nil {
        logger = zap.NewNop()
    }

    if err := os.MkdirAll(dataDir, 0750); err != nil {
        return nil, fmt.Errorf("create data directory: %w", err)
    }

    store := &JSONFileStorage{
        dataDir:       dataDir,
        maxFileSize:   maxFileSize,
        retentionDays: retentionDays,
        logger:        logger,
        knownIDs:      make(map[string]struct{}),
    }

    if err := store.loadEventIDs(); err != nil {
        return nil, fmt.Errorf("load existing events: %w", err)
    }

    return store, nil
}

func (store *JSONFileStorage) Store(
    ctx context.Context,
    event contracts.Event,
) error {
    if err := ctx.Err(); err != nil {
        return err
    }

    if event.EventID == "" {
        return errors.New("event ID is required")
    }

    data, err := json.Marshal(event)
    if err != nil {
        return fmt.Errorf("encode event: %w", err)
    }

    data = append(data, '\n')

    store.mu.Lock()
    defer store.mu.Unlock()

    if store.closed {
        return ErrStoreClosed
    }

    if _, exists := store.knownIDs[event.EventID]; exists {
        return ErrDuplicateEvent
    }

    filename, err := store.fileForWrite(len(data), time.Now().UTC())
    if err != nil {
        return err
    }

    file, err := os.OpenFile(
        filename,
        os.O_CREATE|os.O_WRONLY|os.O_APPEND,
        0600,
    )
    if err != nil {
        return fmt.Errorf("open event file: %w", err)
    }

    _, writeErr := file.Write(data)
    if writeErr == nil {
        // Flush each accepted event so Store returns only after durable handoff.
        writeErr = file.Sync()
    }

    closeErr := file.Close()
    if writeErr != nil {
        return fmt.Errorf("write event: %w", writeErr)
    }
    if closeErr != nil {
        return fmt.Errorf("close event file: %w", closeErr)
    }

    store.knownIDs[event.EventID] = struct{}{}
    return nil
}

func (store *JSONFileStorage) Get(
    ctx context.Context,
    eventID string,
) (contracts.Event, error) {
    if err := ctx.Err(); err != nil {
        return contracts.Event{}, err
    }

    store.mu.RLock()
    defer store.mu.RUnlock()

    if store.closed {
        return contracts.Event{}, ErrStoreClosed
    }

    if _, exists := store.knownIDs[eventID]; !exists {
        return contracts.Event{}, ErrEventNotFound
    }

    files, err := store.eventFiles()
    if err != nil {
        return contracts.Event{}, err
    }

    for _, filename := range files {
        events, err := readEvents(filename)
        if err != nil {
            return contracts.Event{}, err
        }

        for _, event := range events {
            if event.EventID == eventID {
                return event, nil
            }
        }
    }

    return contracts.Event{}, ErrEventNotFound
}

func (store *JSONFileStorage) Query(
    ctx context.Context,
    filter contracts.EventFilter,
) ([]contracts.Event, error) {
    if err := ctx.Err(); err != nil {
        return nil, err
    }

    store.mu.RLock()
    defer store.mu.RUnlock()

    if store.closed {
        return nil, ErrStoreClosed
    }

    files, err := store.eventFiles()
    if err != nil {
        return nil, err
    }

    result := make([]contracts.Event, 0)

    for _, filename := range files {
        events, err := readEvents(filename)
        if err != nil {
            return nil, err
        }

        for _, event := range events {
            if matchesFilter(event, filter) {
                result = append(result, event)
            }
        }
    }

    sort.SliceStable(result, func(i, j int) bool {
        return result[i].Timestamp.Before(result[j].Timestamp)
    })

    return result, nil
}

func (store *JSONFileStorage) Health(ctx context.Context) error {
    if err := ctx.Err(); err != nil {
        return err
    }

    store.mu.RLock()
    defer store.mu.RUnlock()

    if store.closed {
        return ErrStoreClosed
    }

    info, err := os.Stat(store.dataDir)
    if err != nil {
        return err
    }

    if !info.IsDir() {
        return errors.New("event data path is not a directory")
    }

    return nil
}

func (store *JSONFileStorage) Close() error {
    store.mu.Lock()
    defer store.mu.Unlock()

    if store.closed {
        return nil
    }

    store.closed = true
    return nil
}

// Rotate removes files older than retentionDays.
func (store *JSONFileStorage) Rotate(ctx context.Context) error {
    if err := ctx.Err(); err != nil {
        return err
    }

    store.mu.Lock()
    defer store.mu.Unlock()

    if store.closed {
        return ErrStoreClosed
    }

    cutoff := time.Now().UTC().AddDate(0, 0, -store.retentionDays)

    files, err := store.eventFiles()
    if err != nil {
        return err
    }

    for _, filename := range files {
        info, err := os.Stat(filename)
        if err != nil {
            if os.IsNotExist(err) {
                continue
            }
            return err
        }

        if info.ModTime().Before(cutoff) {
            if err := os.Remove(filename); err != nil {
                return fmt.Errorf("remove expired event file: %w", err)
            }

            store.logger.Info("removed expired event file",
                zap.String("file", filename),
            )
        }
    }

    return nil
}

func (store *JSONFileStorage) fileForWrite(
    recordSize int,
    now time.Time,
) (string, error) {
    weekStart := startOfWeek(now)
    prefix := filepath.Join(
        store.dataDir,
        fmt.Sprintf("events_%s", weekStart.Format("20060102")),
    )

    for index := 0; ; index++ {
        filename := fmt.Sprintf("%s_%02d.jsonl", prefix, index)

        info, err := os.Stat(filename)
        if err != nil {
            if os.IsNotExist(err) {
                return filename, nil
            }
            return "", err
        }

        if info.Size()+int64(recordSize) <= store.maxFileSize {
            return filename, nil
        }
    }
}

func (store *JSONFileStorage) loadEventIDs() error {
    files, err := store.eventFiles()
    if err != nil {
        return err
    }

    for _, filename := range files {
        events, err := readEvents(filename)
        if err != nil {
            return err
        }

        for _, event := range events {
            if event.EventID != "" {
                store.knownIDs[event.EventID] = struct{}{}
            }
        }
    }

    return nil
}

func (store *JSONFileStorage) eventFiles() ([]string, error) {
    matches, err := filepath.Glob(
        filepath.Join(store.dataDir, "events_*.jsonl"),
    )
    if err != nil {
        return nil, err
    }

    sort.Strings(matches)
    return matches, nil
}

func readEvents(filename string) ([]contracts.Event, error) {
    file, err := os.Open(filename)
    if err != nil {
        return nil, fmt.Errorf("open event file: %w", err)
    }
    defer file.Close()

    events := make([]contracts.Event, 0)
    scanner := bufio.NewScanner(file)

    // Permit records larger than Scanner's default 64 KiB limit.
    scanner.Buffer(make([]byte, 64*1024), 1024*1024)

    for scanner.Scan() {
        line := scanner.Bytes()
        if len(line) == 0 {
            continue
        }

        var event contracts.Event
        if err := json.Unmarshal(line, &event); err != nil {
            return nil, fmt.Errorf(
                "decode event in %s: %w",
                filename,
                err,
            )
        }

        events = append(events, event)
    }

    if err := scanner.Err(); err != nil {
        return nil, fmt.Errorf("read event file: %w", err)
    }

    return events, nil
}

func startOfWeek(value time.Time) time.Time {
    value = value.UTC()

    // Monday is the start of the storage week.
    daysSinceMonday := (int(value.Weekday()) + 6) % 7
    return value.AddDate(0, 0, -daysSinceMonday).
        Truncate(24 * time.Hour)
}

func matchesFilter(
    event contracts.Event,
    filter contracts.EventFilter,
) bool {
    if len(filter.Levels) > 0 &&
        !containsLevel(filter.Levels, event.Level) {
        return false
    }

    if len(filter.Services) > 0 &&
        !containsString(filter.Services, event.Service) {
        return false
    }

    if len(filter.EventTypes) > 0 &&
        !containsString(filter.EventTypes, event.EventType) {
        return false
    }

    return true
}

func containsLevel(
    values []contracts.EventLevel,
    target contracts.EventLevel,
) bool {
    for _, value := range values {
        if value == target {
            return true
        }
    }

    return false
}

func containsString(values []string, target string) bool {
    for _, value := range values {
        if value == target {
            return true
        }
    }

    return false
}

// Compile-time contract check.
var _ interface {
    Store(context.Context, contracts.Event) error
    Get(context.Context, string) (contracts.Event, error)
    Query(context.Context, contracts.EventFilter) ([]contracts.Event, error)
    Health(context.Context) error
    Close() error
} = (*JSONFileStorage)(nil)

// Keep io imported available for compatibility with older callers that may
// use this file while migrating from array-based JSON storage.
var _ = io.EOF