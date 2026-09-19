package websocket

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	"github.com/gorilla/websocket"
	"github.com/hftamayo/gologger/internal/contracts"
	"github.com/hftamayo/gologger/internal/ports"
)

var (
    ErrHandlerClosed = errors.New("event handler is shutting down")
    ErrQueueFull     = errors.New("event processing queue is full")
)

type eventPublisher struct {
    mu            sync.RWMutex
    subscriptions map[*eventSubscription]struct{}
}

type eventSubscription struct {
    mu      sync.Once
    filter  contracts.EventFilter
    events  chan contracts.Event
}

// EventHandler owns the bounded producer queue and its workers. Socket clients
// submit work through Submit; workers process and persist admitted events, and
// Shutdown drains admitted work before returning.
type EventHandler struct {
    processor ports.EventProcessor
    store     ports.EventStore
    publisher ports.EventPublisher

    jobs chan eventJob
    wg   sync.WaitGroup

    admissionMu sync.Mutex
    accepting   atomic.Bool
    stopOnce    sync.Once
}

type eventJob struct {
    ctx  context.Context
    event contracts.Event
    ack  chan EventResult
}

// EventResult contains the processed event or the processing/persistence error.
type EventResult struct {
    Event contracts.Event
    Err   error
}

func newEventPublisher(bufferSize int) ports.EventPublisher {
    return &eventPublisher{
        subscriptions: make(map[*eventSubscription]struct{}),
    }
}

func (publisher *eventPublisher) Subscribe(
    filter contracts.EventFilter,
) ports.EventSubscription {
    subscription := &eventSubscription{
        filter: filter,
        events: make(chan contracts.Event, 32),
    }

    publisher.mu.Lock()
    publisher.subscriptions[subscription] = struct{}{}
    publisher.mu.Unlock()

    return subscription
}

func (publisher *eventPublisher) Publish(event contracts.Event) {
    publisher.mu.RLock()
    defer publisher.mu.RUnlock()

    for subscription := range publisher.subscriptions {
        if !matchesFilter(event, subscription.filter) {
            continue
        }

        select {
        case subscription.events <- event:
        default:
            // A slow monitor must not block event processing.
        }
    }
}

func (subscription *eventSubscription) Events() <-chan contracts.Event {
    return subscription.events
}

func (subscription *eventSubscription) Close() {
    subscription.mu.Do(func() {
        close(subscription.events)
    })
}

func matchesFilter(
    event contracts.Event,
    filter contracts.EventFilter,
) bool {
    return matchesLevel(event.Level, filter.Levels) &&
        matchesString(event.Service, filter.Services) &&
        matchesString(event.EventType, filter.EventTypes)
}

func matchesLevel(
    value contracts.EventLevel,
    values []contracts.EventLevel,
) bool {
    if len(values) == 0 {
        return true
    }

    for _, candidate := range values {
        if value == candidate {
            return true
        }
    }

    return false
}

func matchesString(value string, values []string) bool {
    if len(values) == 0 {
        return true
    }

    for _, candidate := range values {
        if value == candidate {
            return true
        }
    }

    return false
}

// NewEventHandler starts workers consuming a bounded event queue.
func NewEventHandler(
    processor ports.EventProcessor,
    store ports.EventStore,
    queueSize int,
    workerCount int,
) (*EventHandler, error) {
    if processor == nil {
        return nil, errors.New("event processor is required")
    }
    if store == nil {
        return nil, errors.New("event store is required")
    }
    if queueSize <= 0 {
        return nil, errors.New("queue size must be positive")
    }
    if workerCount <= 0 {
        return nil, errors.New("worker count must be positive")
    }

    handler := &EventHandler{
        processor: processor,
        store:     store,
        publisher: newEventPublisher(32),
        jobs:      make(chan eventJob, queueSize),
    }
    handler.accepting.Store(true)

    handler.wg.Add(workerCount)
    for index := 0; index < workerCount; index++ {
        go handler.worker()
    }

    return handler, nil
}

// Submit admits one producer event or returns ErrQueueFull immediately. The
// returned result must be read by the caller to receive the persistence result.
func (handler *EventHandler) Submit(
    ctx context.Context,
    event contracts.Event,
) (<-chan EventResult, error) {
    if ctx == nil {
        ctx = context.Background()
    }

    result := make(chan EventResult, 1)
    job := eventJob{ctx: ctx, event: event, ack: result}

    handler.admissionMu.Lock()
    defer handler.admissionMu.Unlock()

    if !handler.accepting.Load() {
        return nil, ErrHandlerClosed
    }

    select {
    case handler.jobs <- job:
        return result, nil
    case <-ctx.Done():
        return nil, ctx.Err()
    default:
        return nil, ErrQueueFull
    }
}

func (handler *EventHandler) Subscribe(
    filter contracts.EventFilter,
) ports.EventSubscription {
    return handler.publisher.Subscribe(filter)
}

func (handler *EventHandler) worker() {
    defer handler.wg.Done()

    for job := range handler.jobs {
        processed, err := handler.processor.Process(job.ctx, job.event)
        if err == nil {
            err = handler.store.Store(job.ctx, processed)
        }
        if err == nil {
            handler.publisher.Publish(processed)
        }

        job.ack <- EventResult{
            Event: processed,
            Err:   err,
        }
        close(job.ack)
    }
}

func (handler *EventHandler) handleMonitor(
    connection *websocket.Conn,
    message *contracts.SubscribeMessage,
) {
    subscription := handler.Subscribe(message.Filters)
    defer subscription.Close()

    for event := range subscription.Events() {
        if err := connection.WriteJSON(contracts.EventMessage{
            Type:  contracts.MessageTypeEvent,
            Event: event,
        }); err != nil {
            return
        }
    }
}

// Shutdown stops admission, drains queued events until the context expires,
// and returns the number of events that could not be processed.
func (handler *EventHandler) Shutdown(ctx context.Context) (int, error) {
    if ctx == nil {
        ctx = context.Background()
    }

    handler.stopOnce.Do(func() {
        handler.admissionMu.Lock()
        handler.accepting.Store(false)
        close(handler.jobs)
        handler.admissionMu.Unlock()
    })

    done := make(chan struct{})
    go func() {
        handler.wg.Wait()
        close(done)
    }()

    select {
    case <-done:
        return 0, nil
    case <-ctx.Done():
        return len(handler.jobs), ctx.Err()
    }
}