package websocket

import (
	"sync"

	"github.com/hftamayo/gologger/internal/contracts"
	"github.com/hftamayo/gologger/internal/ports"
)

type eventPublisher struct {
	mu            sync.RWMutex
	bufferSize    int
	subscriptions map[*eventSubscription]struct{}
}

type eventSubscription struct {
	mu        sync.Once
	publisher *eventPublisher
	filter    contracts.EventFilter
	events    chan contracts.Event
}

func NewEventPublisher(bufferSize int) ports.EventPublisher {
	if bufferSize <= 0 {
		bufferSize = 32
	}

	return &eventPublisher{
		bufferSize:    bufferSize,
		subscriptions: make(map[*eventSubscription]struct{}),
	}
}

func (publisher *eventPublisher) Subscribe(
	filter contracts.EventFilter,
) ports.EventSubscription {
	subscription := &eventSubscription{
		publisher: publisher,
		filter:    filter,
		events:    make(chan contracts.Event, publisher.bufferSize),
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
		subscription.publisher.mu.Lock()
		delete(subscription.publisher.subscriptions, subscription)
		close(subscription.events)
		subscription.publisher.mu.Unlock()
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
