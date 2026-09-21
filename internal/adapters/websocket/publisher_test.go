package websocket

import (
	"testing"
	"time"

	"github.com/hftamayo/gologger/internal/contracts"
)

func TestEventPublisherPublishesMatchingEvent(t *testing.T) {
	publisher := NewEventPublisher(1)

	subscription := publisher.Subscribe(contracts.EventFilter{
		Levels:     []contracts.EventLevel{contracts.EventLevelError},
		Services:   []string{"orders"},
		EventTypes: []string{"order_failed"},
	})
	defer subscription.Close()

	event := testPublisherEvent(
		"event-1",
		contracts.EventLevelError,
		"orders",
		"order_failed",
	)

	publisher.Publish(event)

	result := receiveEvent(t, subscription.Events())

	if result.EventID != event.EventID {
		t.Fatalf("expected event ID %q, got %q", event.EventID, result.EventID)
	}
}

func TestEventPublisherDoesNotPublishNonMatchingLevel(t *testing.T) {
	publisher := NewEventPublisher(1)

	subscription := publisher.Subscribe(contracts.EventFilter{
		Levels: []contracts.EventLevel{contracts.EventLevelError},
	})
	defer subscription.Close()

	publisher.Publish(testPublisherEvent(
		"event-1",
		contracts.EventLevelInfo,
		"orders",
		"order_created",
	))

	assertNoEvent(t, subscription.Events())
}

func TestEventPublisherDoesNotPublishNonMatchingService(t *testing.T) {
	publisher := NewEventPublisher(1)

	subscription := publisher.Subscribe(contracts.EventFilter{
		Services: []string{"payments"},
	})
	defer subscription.Close()

	publisher.Publish(testPublisherEvent(
		"event-1",
		contracts.EventLevelInfo,
		"orders",
		"order_created",
	))

	assertNoEvent(t, subscription.Events())
}

func TestEventPublisherDoesNotPublishNonMatchingEventType(t *testing.T) {
	publisher := NewEventPublisher(1)

	subscription := publisher.Subscribe(contracts.EventFilter{
		EventTypes: []string{"payment_failed"},
	})
	defer subscription.Close()

	publisher.Publish(testPublisherEvent(
		"event-1",
		contracts.EventLevelError,
		"orders",
		"order_failed",
	))

	assertNoEvent(t, subscription.Events())
}

func TestEventPublisherPublishesWhenFilterIsEmpty(t *testing.T) {
	publisher := NewEventPublisher(1)

	subscription := publisher.Subscribe(contracts.EventFilter{})
	defer subscription.Close()

	event := testPublisherEvent(
		"event-1",
		contracts.EventLevelInfo,
		"orders",
		"order_created",
	)

	publisher.Publish(event)

	result := receiveEvent(t, subscription.Events())

	if result.EventID != event.EventID {
		t.Fatalf("expected event ID %q, got %q", event.EventID, result.EventID)
	}
}

func TestEventPublisherDoesNotBlockWhenSubscriptionBufferIsFull(t *testing.T) {
	publisher := NewEventPublisher(1)

	subscription := publisher.Subscribe(contracts.EventFilter{})
	defer subscription.Close()

	first := testPublisherEvent(
		"event-1",
		contracts.EventLevelInfo,
		"orders",
		"order_created",
	)
	second := testPublisherEvent(
		"event-2",
		contracts.EventLevelError,
		"orders",
		"order_failed",
	)

	publisher.Publish(first)

	done := make(chan struct{})
	go func() {
		publisher.Publish(second)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Publish blocked when subscription buffer was full")
	}

	result := receiveEvent(t, subscription.Events())
	if result.EventID != first.EventID {
		t.Fatalf("expected first event to remain queued, got %q", result.EventID)
	}

	assertNoEvent(t, subscription.Events())
}

func TestEventSubscriptionCloseClosesEventsChannel(t *testing.T) {
	publisher := NewEventPublisher(1)

	subscription := publisher.Subscribe(contracts.EventFilter{})
	subscription.Close()

	_, ok := <-subscription.Events()
	if ok {
		t.Fatal("expected subscription events channel to be closed")
	}
}

func TestEventSubscriptionCloseUnsubscribesFromPublisher(t *testing.T) {
	publisher := NewEventPublisher(1)

	subscription := publisher.Subscribe(contracts.EventFilter{})
	subscription.Close()

	defer func() {
		if recovered := recover(); recovered != nil {
			t.Fatalf("Publish panicked after subscription close: %v", recovered)
		}
	}()

	publisher.Publish(testPublisherEvent(
		"event-1",
		contracts.EventLevelInfo,
		"orders",
		"order_created",
	))
}

func TestEventSubscriptionCloseIsIdempotent(t *testing.T) {
	publisher := NewEventPublisher(1)

	subscription := publisher.Subscribe(contracts.EventFilter{})

	subscription.Close()
	subscription.Close()
}

func TestNewEventPublisherUsesDefaultBufferSize(t *testing.T) {
	publisher := NewEventPublisher(0)

	subscription := publisher.Subscribe(contracts.EventFilter{})
	defer subscription.Close()

	for index := 0; index < 32; index++ {
		publisher.Publish(testPublisherEvent(
			"event",
			contracts.EventLevelInfo,
			"orders",
			"order_created",
		))
	}

	done := make(chan struct{})
	go func() {
		publisher.Publish(testPublisherEvent(
			"event-overflow",
			contracts.EventLevelInfo,
			"orders",
			"order_created",
		))
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Publish blocked with default buffer size")
	}
}

func receiveEvent(
	t *testing.T,
	events <-chan contracts.Event,
) contracts.Event {
	t.Helper()

	select {
	case event, ok := <-events:
		if !ok {
			t.Fatal("expected event, channel was closed")
		}

		return event

	case <-time.After(100 * time.Millisecond):
		t.Fatal("timed out waiting for event")
	}

	return contracts.Event{}
}

func assertNoEvent(
	t *testing.T,
	events <-chan contracts.Event,
) {
	t.Helper()

	select {
	case event, ok := <-events:
		if !ok {
			t.Fatal("events channel was unexpectedly closed")
		}

		t.Fatalf("expected no event, got %#v", event)

	case <-time.After(25 * time.Millisecond):
	}
}

func testPublisherEvent(
	eventID string,
	level contracts.EventLevel,
	service string,
	eventType string,
) contracts.Event {
	return contracts.Event{
		EventID:   eventID,
		Level:     level,
		Service:   service,
		EventType: eventType,
		Message:   "fixture event",
	}
}
