package storage

import (
	"context"
	"errors"
	"sync"

	"github.com/hftamayo/gologger/internal/contracts"
)

var (
	ErrEventNotFound = errors.New("event not found")
	ErrStoreClosed   = errors.New("event store is closed")
	ErrDuplicateEvent = errors.New("event already exists")
)

// MemoryEventStore stores events safely in memory.
type MemoryEventStore struct {
	mu     sync.RWMutex
	events map[string]contracts.Event
	order  []string
	closed bool
}

// NewMemoryEventStore creates an empty in-memory event store.
func NewMemoryEventStore() *MemoryEventStore {
	return &MemoryEventStore{
		events: make(map[string]contracts.Event),
		order:  make([]string, 0),
	}
}

func (store *MemoryEventStore) Store(
	ctx context.Context,
	event contracts.Event,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	if event.EventID == "" {
		return errors.New("event ID is required")
	}

	store.mu.Lock()
	defer store.mu.Unlock()

	if store.closed {
		return ErrStoreClosed
	}

	if _, exists := store.events[event.EventID]; exists {
		return ErrDuplicateEvent
	}

	store.events[event.EventID] = cloneEvent(event)
	store.order = append(store.order, event.EventID)

	return nil
}

func (store *MemoryEventStore) Get(
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

	event, exists := store.events[eventID]
	if !exists {
		return contracts.Event{}, ErrEventNotFound
	}

	return cloneEvent(event), nil
}

func (store *MemoryEventStore) Query(
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

	events := make([]contracts.Event, 0)

	for _, eventID := range store.order {
		event := store.events[eventID]

		if !matchesFilter(event, filter) {
			continue
		}

		events = append(events, cloneEvent(event))
	}

	return events, nil
}

func (store *MemoryEventStore) Health(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	store.mu.RLock()
	defer store.mu.RUnlock()

	if store.closed {
		return ErrStoreClosed
	}

	return nil
}

func (store *MemoryEventStore) Close() error {
	store.mu.Lock()
	defer store.mu.Unlock()

	if store.closed {
		return nil
	}

	store.closed = true
	return nil
}

func matchesFilter(
	event contracts.Event,
	filter contracts.EventFilter,
) bool {
	if len(filter.Levels) > 0 && !containsLevel(filter.Levels, event.Level) {
		return false
	}

	if len(filter.Services) > 0 && !containsString(filter.Services, event.Service) {
		return false
	}

	if len(filter.EventTypes) > 0 &&
		!containsString(filter.EventTypes, event.EventType) {
		return false
	}

	return true
}

func containsLevel(
	levels []contracts.EventLevel,
	value contracts.EventLevel,
) bool {
	for _, level := range levels {
		if level == value {
			return true
		}
	}

	return false
}

func containsString(values []string, value string) bool {
	for _, candidate := range values {
		if candidate == value {
			return true
		}
	}

	return false
}

func cloneEvent(event contracts.Event) contracts.Event {
	cloned := event

	if event.Context != nil {
		cloned.Context = cloneMap(event.Context)
	}

	if event.Metadata != nil {
		cloned.Metadata = cloneMap(event.Metadata)
	}

	return cloned
}

func cloneMap(values map[string]any) map[string]any {
	cloned := make(map[string]any, len(values))

	for key, value := range values {
		switch typed := value.(type) {
		case map[string]any:
			cloned[key] = cloneMap(typed)

		case []any:
			items := make([]any, len(typed))
			for index, item := range typed {
				if nested, ok := item.(map[string]any); ok {
					items[index] = cloneMap(nested)
				} else {
					items[index] = item
				}
			}
			cloned[key] = items

		default:
			cloned[key] = value
		}
	}

	return cloned
}
