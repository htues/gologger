package websocket

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/hftamayo/gologger/internal/contracts"
)

type testProcessor struct{}

type blockingProcessor struct {
	started chan struct{}
	release chan struct{}
}

func (testProcessor) Process(
	ctx context.Context,
	event contracts.Event,
) (contracts.Event, error) {
	if err := ctx.Err(); err != nil {
		return contracts.Event{}, err
	}

	return event, nil
}

func (processor *blockingProcessor) Process(
	ctx context.Context,
	event contracts.Event,
) (contracts.Event, error) {
	close(processor.started)

	select {
	case <-processor.release:
		return event, nil

	case <-ctx.Done():
		return contracts.Event{}, ctx.Err()
	}
}

type testStore struct {
	stored chan contracts.Event
}

func (store *testStore) Store(
	ctx context.Context,
	event contracts.Event,
) error {
	select {
	case store.stored <- event:
		return nil

	case <-ctx.Done():
		return ctx.Err()
	}
}

func (store *testStore) Get(
	context.Context,
	string,
) (contracts.Event, error) {
	return contracts.Event{}, errors.New("not implemented")
}

func (store *testStore) Query(
	context.Context,
	contracts.EventFilter,
) ([]contracts.Event, error) {
	return nil, errors.New("not implemented")
}

func (*testStore) Health(context.Context) error { return nil }
func (*testStore) Close() error                 { return nil }

func TestDispatcherRejectsWhenQueueIsFull(t *testing.T) {
	store := &testStore{stored: make(chan contracts.Event, 2)}
	processor := &blockingProcessor{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}

	dispatcher, err := NewDispatcher(processor, store, 1, 1)
	if err != nil {
		t.Fatalf("NewDispatcher returned error: %v", err)
	}

	first, err := dispatcher.Submit(context.Background(), testEvent("first"))
	if err != nil {
		t.Fatalf("first Submit returned error: %v", err)
	}

	<-processor.started

	if _, err = dispatcher.Submit(
		context.Background(),
		testEvent("second"),
	); err != nil {
		t.Fatalf("second Submit returned error: %v", err)
	}

	_, err = dispatcher.Submit(context.Background(), testEvent("third"))
	if !errors.Is(err, ErrQueueFull) {
		t.Fatalf("expected ErrQueueFull, got %v", err)
	}

	close(processor.release)

	if result := <-first; result.Err != nil {
		t.Fatalf("first event failed: %v", result.Err)
	}

	shutdownContext, cancel := context.WithTimeout(
		context.Background(),
		time.Second,
	)
	defer cancel()

	if _, err := dispatcher.Shutdown(shutdownContext); err != nil {
		t.Fatalf("Shutdown returned error: %v", err)
	}
}

func TestDispatcherDrainsAcceptedEvents(t *testing.T) {
	store := &testStore{stored: make(chan contracts.Event, 2)}

	dispatcher, err := NewDispatcher(testProcessor{}, store, 2, 1)
	if err != nil {
		t.Fatalf("NewDispatcher returned error: %v", err)
	}

	first, err := dispatcher.Submit(context.Background(), testEvent("first"))
	if err != nil {
		t.Fatalf("first Submit returned error: %v", err)
	}

	second, err := dispatcher.Submit(context.Background(), testEvent("second"))
	if err != nil {
		t.Fatalf("second Submit returned error: %v", err)
	}

	shutdownContext, cancel := context.WithTimeout(
		context.Background(),
		time.Second,
	)
	defer cancel()

	undrained, err := dispatcher.Shutdown(shutdownContext)
	if err != nil {
		t.Fatalf("Shutdown returned error: %v", err)
	}

	if undrained != 0 {
		t.Fatalf("expected no undrained events, got %d", undrained)
	}

	if result := <-first; result.Err != nil {
		t.Fatalf("first event failed: %v", result.Err)
	}

	if result := <-second; result.Err != nil {
		t.Fatalf("second event failed: %v", result.Err)
	}

	if got := len(store.stored); got != 2 {
		t.Fatalf("expected two stored events, got %d", got)
	}
}

func testEvent(eventID string) contracts.Event {
	return contracts.Event{
		EventID:   eventID,
		Level:     contracts.EventLevelInfo,
		Service:   "test-service",
		EventType: "test_event",
		Message:   "test message",
	}
}
