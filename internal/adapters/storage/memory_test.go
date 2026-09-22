package storage_test

import (
	"context"
	"errors"
	"testing"

	"github.com/hftamayo/gologger/internal/adapters/storage"
	"github.com/hftamayo/gologger/internal/contracts"
)

func TestMemoryEventStoreStoresAndGetsEvent(t *testing.T) {
	store := storage.NewMemoryEventStore()
	event := testEvent("evt-1")

	if err := store.Store(context.Background(), event); err != nil {
		t.Fatalf("Store returned error: %v", err)
	}

	result, err := store.Get(context.Background(), event.EventID)
	if err != nil {
		t.Fatalf("Get returned error: %v", err)
	}

	if result.EventID != event.EventID {
		t.Fatalf("expected event ID %q, got %q", event.EventID, result.EventID)
	}

	if result.Message != event.Message {
		t.Fatalf("expected message %q, got %q", event.Message, result.Message)
	}
}

func TestMemoryEventStoreRejectsDuplicateEvent(t *testing.T) {
	store := storage.NewMemoryEventStore()
	event := testEvent("evt-1")

	if err := store.Store(context.Background(), event); err != nil {
		t.Fatalf("first Store returned error: %v", err)
	}

	if err := store.Store(context.Background(), event); !errors.Is(
		err,
		storage.ErrDuplicateEvent,
	) {
		t.Fatalf("expected duplicate error, got %v", err)
	}
}

func TestMemoryEventStoreQueriesWithFilters(t *testing.T) {
	store := storage.NewMemoryEventStore()

	events := []contracts.Event{
		testEvent("evt-1"),
		{
			EventID:   "evt-2",
			Level:     contracts.EventLevelError,
			Service:   "users",
			EventType: "user_created",
			Message:   "User created",
		},
		{
			EventID:   "evt-3",
			Level:     contracts.EventLevelWarn,
			Service:   "payments",
			EventType: "payment_delayed",
			Message:   "Payment delayed",
		},
	}

	for _, event := range events {
		if err := store.Store(context.Background(), event); err != nil {
			t.Fatalf("Store returned error: %v", err)
		}
	}

	result, err := store.Query(context.Background(), contracts.EventFilter{
		Services: []string{"payments"},
		Levels:   []contracts.EventLevel{contracts.EventLevelInfo},
	})
	if err != nil {
		t.Fatalf("Query returned error: %v", err)
	}

	if len(result) != 1 {
		t.Fatalf("expected one matching event, got %d", len(result))
	}

	if result[0].EventID != "evt-1" {
		t.Fatalf("expected evt-1, got %q", result[0].EventID)
	}
}

func TestMemoryEventStoreReturnsCopies(t *testing.T) {
	store := storage.NewMemoryEventStore()
	event := testEvent("evt-1")
	event.Metadata = map[string]any{
		"orderId": "order-1",
	}

	if err := store.Store(context.Background(), event); err != nil {
		t.Fatalf("Store returned error: %v", err)
	}

	result, err := store.Get(context.Background(), event.EventID)
	if err != nil {
		t.Fatalf("Get returned error: %v", err)
	}

	result.Metadata["orderId"] = "changed"

	stored, err := store.Get(context.Background(), event.EventID)
	if err != nil {
		t.Fatalf("second Get returned error: %v", err)
	}

	if stored.Metadata["orderId"] != "order-1" {
		t.Fatal("store returned an internal mutable map")
	}
}

func TestMemoryEventStoreCloses(t *testing.T) {
	store := storage.NewMemoryEventStore()

	if err := store.Close(); err != nil {
		t.Fatalf("Close returned error: %v", err)
	}

	if err := store.Health(context.Background()); !errors.Is(
		err,
		storage.ErrStoreClosed,
	) {
		t.Fatalf("expected closed store error, got %v", err)
	}
}

func testEvent(eventID string) contracts.Event {
	return contracts.Event{
		EventID:   eventID,
		Level:     contracts.EventLevelInfo,
		Service:   "payments",
		EventType: "payment_declined",
		Message:   "Payment declined",
	}
}
