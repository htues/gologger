package websocket

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	"github.com/hftamayo/gologger/internal/contracts"
	"github.com/hftamayo/gologger/internal/ports"
)

var (
	ErrHandlerClosed = errors.New("event dispatcher is shutting down")
	ErrQueueFull     = errors.New("event processing queue is full")
)

type Dispatcher struct {
	processor ports.EventProcessor
	store     ports.EventStore
	publisher ports.EventPublisher

	jobs chan eventJob
	wg   sync.WaitGroup

	admissionMu sync.Mutex
	accepting   atomic.Bool
	stopOnce    sync.Once
}

type EventResult struct {
	Event contracts.Event
	Err   error
}

type eventJob struct {
	ctx   context.Context
	event contracts.Event
	ack   chan EventResult
}

func NewDispatcher(
	processor ports.EventProcessor,
	store ports.EventStore,
	queueSize int,
	workerCount int,
) (*Dispatcher, error) {
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

	dispatcher := &Dispatcher{
		processor: processor,
		store:     store,
		publisher: NewEventPublisher(32),
		jobs:      make(chan eventJob, queueSize),
	}
	dispatcher.accepting.Store(true)

	dispatcher.wg.Add(workerCount)
	for index := 0; index < workerCount; index++ {
		go dispatcher.worker()
	}

	return dispatcher, nil
}

func (dispatcher *Dispatcher) Submit(
	ctx context.Context,
	event contracts.Event,
) (<-chan EventResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	result := make(chan EventResult, 1)
	job := eventJob{
		ctx:   ctx,
		event: event,
		ack:   result,
	}

	dispatcher.admissionMu.Lock()
	defer dispatcher.admissionMu.Unlock()

	if !dispatcher.accepting.Load() {
		return nil, ErrHandlerClosed
	}

	select {
	case dispatcher.jobs <- job:
		return result, nil

	case <-ctx.Done():
		return nil, ctx.Err()

	default:
		return nil, ErrQueueFull
	}
}

func (dispatcher *Dispatcher) Subscribe(
	filter contracts.EventFilter,
) ports.EventSubscription {
	return dispatcher.publisher.Subscribe(filter)
}

func (dispatcher *Dispatcher) Shutdown(ctx context.Context) (int, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	dispatcher.stopOnce.Do(func() {
		dispatcher.admissionMu.Lock()
		dispatcher.accepting.Store(false)
		close(dispatcher.jobs)
		dispatcher.admissionMu.Unlock()
	})

	done := make(chan struct{})
	go func() {
		dispatcher.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return 0, nil

	case <-ctx.Done():
		return len(dispatcher.jobs), ctx.Err()
	}
}

func (dispatcher *Dispatcher) worker() {
	defer dispatcher.wg.Done()

	for job := range dispatcher.jobs {
		processed, err := dispatcher.processor.Process(job.ctx, job.event)
		if err == nil {
			err = dispatcher.store.Store(job.ctx, processed)
		}
		if err == nil {
			dispatcher.publisher.Publish(processed)
		}

		job.ack <- EventResult{
			Event: processed,
			Err:   err,
		}
		close(job.ack)
	}
}
