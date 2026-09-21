package websocket

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/hftamayo/gologger/internal/contracts"
)

type fakeEventProcessor struct {
	mu           sync.Mutex
	processFunc  func(ctx context.Context, event contracts.Event) (contracts.Event, error)
	processed    []contracts.Event
	processCalls int
}

func (processor *fakeEventProcessor) Process(
	ctx context.Context,
	event contracts.Event,
) (contracts.Event, error) {
	processor.mu.Lock()
	processor.processCalls++
	processor.processed = append(processor.processed, event)
	processor.mu.Unlock()

	if processor.processFunc != nil {
		return processor.processFunc(ctx, event)
	}

	event.EventID = "processed-event-id"
	return event, nil
}

type fakeEventStore struct {
	mu         sync.Mutex
	storeFunc  func(ctx context.Context, event contracts.Event) error
	stored     []contracts.Event
	storeCalls int
}

func (store *fakeEventStore) Store(
	ctx context.Context,
	event contracts.Event,
) error {
	store.mu.Lock()
	store.storeCalls++
	store.stored = append(store.stored, event)
	store.mu.Unlock()

	if store.storeFunc != nil {
		return store.storeFunc(ctx, event)
	}

	return nil
}

func (store *fakeEventStore) Get(
	ctx context.Context,
	eventID string,
) (contracts.Event, error) {
	return contracts.Event{}, nil
}

func (store *fakeEventStore) Query(
	ctx context.Context,
	filter contracts.EventFilter,
) ([]contracts.Event, error) {
	return nil, nil
}

func (store *fakeEventStore) Health(ctx context.Context) error {
	return nil
}

func (store *fakeEventStore) Close() error {
	return nil
}

func TestNewDispatcherValidatesRequiredArguments(t *testing.T) {
	t.Parallel()

	validProcessor := &fakeEventProcessor{}
	validStore := &fakeEventStore{}

	tests := []struct {
		name        string
		processor   *fakeEventProcessor
		store       *fakeEventStore
		queueSize   int
		workerCount int
		wantErr     string
	}{
		{
			name:        "missing processor",
			processor:   nil,
			store:       validStore,
			queueSize:   1,
			workerCount: 1,
			wantErr:     "event processor is required",
		},
		{
			name:        "missing store",
			processor:   validProcessor,
			store:       nil,
			queueSize:   1,
			workerCount: 1,
			wantErr:     "event store is required",
		},
		{
			name:        "invalid queue size",
			processor:   validProcessor,
			store:       validStore,
			queueSize:   0,
			workerCount: 1,
			wantErr:     "queue size must be positive",
		},
		{
			name:        "invalid worker count",
			processor:   validProcessor,
			store:       validStore,
			queueSize:   1,
			workerCount: 0,
			wantErr:     "worker count must be positive",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dispatcher, err := NewDispatcher(
				tt.processor,
				tt.store,
				tt.queueSize,
				tt.workerCount,
			)

			if err == nil {
				t.Fatal("expected error, got nil")
			}

			if err.Error() != tt.wantErr {
				t.Fatalf("expected error %q, got %q", tt.wantErr, err.Error())
			}

			if dispatcher != nil {
				t.Fatal("expected dispatcher to be nil")
			}
		})
	}
}

func TestDispatcherSubmitProcessesStoresAndPublishesEvent(t *testing.T) {
	processor := &fakeEventProcessor{
		processFunc: func(ctx context.Context, event contracts.Event) (contracts.Event, error) {
			event.EventID = "event-123"
			event.Message = "processed message"
			return event, nil
		},
	}
	store := &fakeEventStore{}

	dispatcher, err := NewDispatcher(processor, store, 1, 1)
	if err != nil {
		t.Fatalf("expected dispatcher, got error: %v", err)
	}
	defer shutdownDispatcher(t, dispatcher)

	subscription := dispatcher.Subscribe(contracts.EventFilter{})
	defer subscription.Close()

	input := contracts.Event{
		Level:     contracts.EventLevelInfo,
		Service:   "api",
		EventType: "created",
		Message:   "original message",
	}

	resultCh, err := dispatcher.Submit(context.Background(), input)
	if err != nil {
		t.Fatalf("expected submit to succeed, got error: %v", err)
	}

	result := receiveResult(t, resultCh)

	if result.Err != nil {
		t.Fatalf("expected result without error, got: %v", result.Err)
	}

	if result.Event.EventID != "event-123" {
		t.Fatalf("expected processed event ID, got %q", result.Event.EventID)
	}

	if result.Event.Message != "processed message" {
		t.Fatalf("expected processed message, got %q", result.Event.Message)
	}

	store.mu.Lock()
	storeCalls := store.storeCalls
	stored := append([]contracts.Event(nil), store.stored...)
	store.mu.Unlock()

	if storeCalls != 1 {
		t.Fatalf("expected store to be called once, got %d", storeCalls)
	}

	if stored[0].EventID != "event-123" {
		t.Fatalf("expected stored processed event, got event ID %q", stored[0].EventID)
	}

	select {
	case published := <-subscription.Events():
		if published.EventID != "event-123" {
			t.Fatalf("expected published processed event, got event ID %q", published.EventID)
		}

	case <-time.After(time.Second):
		t.Fatal("timed out waiting for published event")
	}
}

func TestDispatcherSubmitReturnsProcessorError(t *testing.T) {
	expectedErr := errors.New("processor failed")

	processor := &fakeEventProcessor{
		processFunc: func(ctx context.Context, event contracts.Event) (contracts.Event, error) {
			return contracts.Event{}, expectedErr
		},
	}
	store := &fakeEventStore{}

	dispatcher, err := NewDispatcher(processor, store, 1, 1)
	if err != nil {
		t.Fatalf("expected dispatcher, got error: %v", err)
	}
	defer shutdownDispatcher(t, dispatcher)

	resultCh, err := dispatcher.Submit(context.Background(), contracts.Event{
		Level:     contracts.EventLevelError,
		Service:   "api",
		EventType: "failed",
		Message:   "boom",
	})
	if err != nil {
		t.Fatalf("expected submit to succeed, got error: %v", err)
	}

	result := receiveResult(t, resultCh)

	if !errors.Is(result.Err, expectedErr) {
		t.Fatalf("expected processor error %v, got %v", expectedErr, result.Err)
	}

	store.mu.Lock()
	storeCalls := store.storeCalls
	store.mu.Unlock()

	if storeCalls != 0 {
		t.Fatalf("expected store not to be called, got %d calls", storeCalls)
	}
}

func TestDispatcherSubmitReturnsStoreError(t *testing.T) {
	expectedErr := errors.New("store failed")

	processor := &fakeEventProcessor{
		processFunc: func(ctx context.Context, event contracts.Event) (contracts.Event, error) {
			event.EventID = "event-456"
			return event, nil
		},
	}
	store := &fakeEventStore{
		storeFunc: func(ctx context.Context, event contracts.Event) error {
			return expectedErr
		},
	}

	dispatcher, err := NewDispatcher(processor, store, 1, 1)
	if err != nil {
		t.Fatalf("expected dispatcher, got error: %v", err)
	}
	defer shutdownDispatcher(t, dispatcher)

	resultCh, err := dispatcher.Submit(context.Background(), contracts.Event{
		Level:     contracts.EventLevelInfo,
		Service:   "api",
		EventType: "stored",
		Message:   "store me",
	})
	if err != nil {
		t.Fatalf("expected submit to succeed, got error: %v", err)
	}

	result := receiveResult(t, resultCh)

	if !errors.Is(result.Err, expectedErr) {
		t.Fatalf("expected store error %v, got %v", expectedErr, result.Err)
	}

	if result.Event.EventID != "event-456" {
		t.Fatalf("expected processed event in result, got event ID %q", result.Event.EventID)
	}
}

func TestDispatcherSubmitReturnsQueueFull(t *testing.T) {
	blockProcessor := make(chan struct{})

	processor := &fakeEventProcessor{
		processFunc: func(ctx context.Context, event contracts.Event) (contracts.Event, error) {
			<-blockProcessor
			return event, nil
		},
	}
	store := &fakeEventStore{}

	dispatcher, err := NewDispatcher(processor, store, 1, 1)
	if err != nil {
		t.Fatalf("expected dispatcher, got error: %v", err)
	}

	defer func() {
		close(blockProcessor)
		shutdownDispatcher(t, dispatcher)
	}()

	firstResult, err := dispatcher.Submit(context.Background(), contracts.Event{
		Level:     contracts.EventLevelInfo,
		Service:   "api",
		EventType: "first",
		Message:   "first",
	})
	if err != nil {
		t.Fatalf("expected first submit to succeed, got error: %v", err)
	}

	if firstResult == nil {
		t.Fatal("expected first result channel")
	}

	secondResult, err := dispatcher.Submit(context.Background(), contracts.Event{
		Level:     contracts.EventLevelInfo,
		Service:   "api",
		EventType: "second",
		Message:   "second",
	})
	if err != nil {
		t.Fatalf("expected second submit to fill queue, got error: %v", err)
	}

	if secondResult == nil {
		t.Fatal("expected second result channel")
	}

	thirdResult, err := dispatcher.Submit(context.Background(), contracts.Event{
		Level:     contracts.EventLevelInfo,
		Service:   "api",
		EventType: "third",
		Message:   "third",
	})

	if !errors.Is(err, ErrQueueFull) {
		t.Fatalf("expected ErrQueueFull, got %v", err)
	}

	if thirdResult != nil {
		t.Fatal("expected third result channel to be nil")
	}
}

func TestDispatcherSubmitAfterShutdownReturnsHandlerClosed(t *testing.T) {
	processor := &fakeEventProcessor{}
	store := &fakeEventStore{}

	dispatcher, err := NewDispatcher(processor, store, 1, 1)
	if err != nil {
		t.Fatalf("expected dispatcher, got error: %v", err)
	}

	shutdownDispatcher(t, dispatcher)

	resultCh, err := dispatcher.Submit(context.Background(), contracts.Event{
		Level:     contracts.EventLevelInfo,
		Service:   "api",
		EventType: "closed",
		Message:   "closed",
	})

	if !errors.Is(err, ErrHandlerClosed) {
		t.Fatalf("expected ErrHandlerClosed, got %v", err)
	}

	if resultCh != nil {
		t.Fatal("expected result channel to be nil")
	}
}

func TestDispatcherShutdownReturnsPendingJobsWhenContextExpires(t *testing.T) {
	blockProcessor := make(chan struct{})

	processor := &fakeEventProcessor{
		processFunc: func(ctx context.Context, event contracts.Event) (contracts.Event, error) {
			<-blockProcessor
			return event, nil
		},
	}
	store := &fakeEventStore{}

	dispatcher, err := NewDispatcher(processor, store, 2, 1)
	if err != nil {
		t.Fatalf("expected dispatcher, got error: %v", err)
	}

	defer func() {
		close(blockProcessor)

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		_, _ = dispatcher.Shutdown(ctx)
	}()

	_, err = dispatcher.Submit(context.Background(), contracts.Event{
		Level:     contracts.EventLevelInfo,
		Service:   "api",
		EventType: "first",
		Message:   "first",
	})
	if err != nil {
		t.Fatalf("expected first submit to succeed, got error: %v", err)
	}

	_, err = dispatcher.Submit(context.Background(), contracts.Event{
		Level:     contracts.EventLevelInfo,
		Service:   "api",
		EventType: "second",
		Message:   "second",
	})
	if err != nil {
		t.Fatalf("expected second submit to succeed, got error: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	pending, err := dispatcher.Shutdown(ctx)

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context deadline exceeded, got %v", err)
	}

	if pending != 1 {
		t.Fatalf("expected 1 pending job, got %d", pending)
	}
}

func receiveResult(
	t *testing.T,
	resultCh <-chan EventResult,
) EventResult {
	t.Helper()

	select {
	case result, ok := <-resultCh:
		if !ok {
			t.Fatal("expected result channel to be open before receiving result")
		}

		return result

	case <-time.After(time.Second):
		t.Fatal("timed out waiting for dispatcher result")
		return EventResult{}
	}
}

func shutdownDispatcher(
	t *testing.T,
	dispatcher *Dispatcher,
) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	pending, err := dispatcher.Shutdown(ctx)
	if err != nil {
		t.Fatalf("expected shutdown to succeed, got pending=%d err=%v", pending, err)
	}
}
