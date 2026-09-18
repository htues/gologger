package websockets

import (
	"errors"
	"testing"

	"github.com/hftamayo/gologger/internal/contracts"
)

func TestDecodeMessageHello(t *testing.T) {
    messageType, payload, err := decodeMessage([]byte(`{
        "type": "hello",
        "mode": "producer",
        "client": "fixture-client",
        "version": "1.0"
    }`))
    if err != nil {
        t.Fatalf("decodeMessage returned error: %v", err)
    }

    if messageType != contracts.MessageTypeHello {
        t.Fatalf(
            "expected message type %q, got %q",
            contracts.MessageTypeHello,
            messageType,
        )
    }

    hello, ok := payload.(*contracts.HelloMessage)
    if !ok {
        t.Fatalf("expected *HelloMessage, got %T", payload)
    }

    if hello.Mode != contracts.ConnectionModeProducer {
        t.Fatalf("expected producer mode, got %q", hello.Mode)
    }

    if hello.Client != "fixture-client" {
        t.Fatalf("expected fixture client, got %q", hello.Client)
    }
}

func TestDecodeMessageEvent(t *testing.T) {
    messageType, payload, err := decodeMessage([]byte(`{
        "type": "event",
        "requestId": "request-1",
        "event": {
            "eventId": "event-1",
            "level": "info",
            "service": "orders",
            "eventType": "order_created",
            "message": "fixture event"
        }
    }`))
    if err != nil {
        t.Fatalf("decodeMessage returned error: %v", err)
    }

    if messageType != contracts.MessageTypeEvent {
        t.Fatalf(
            "expected message type %q, got %q",
            contracts.MessageTypeEvent,
            messageType,
        )
    }

    eventMessage, ok := payload.(*contracts.EventMessage)
    if !ok {
        t.Fatalf("expected *EventMessage, got %T", payload)
    }

    if eventMessage.RequestID != "request-1" {
        t.Fatalf("expected request ID request-1, got %q", eventMessage.RequestID)
    }

    if eventMessage.Event.EventID != "event-1" {
        t.Fatalf(
            "expected event ID event-1, got %q",
            eventMessage.Event.EventID,
        )
    }
}

func TestDecodeMessageSubscribe(t *testing.T) {
    messageType, payload, err := decodeMessage([]byte(`{
        "type": "subscribe",
        "requestId": "subscription-1",
        "filters": {
            "levels": ["error"],
            "services": ["orders"],
            "eventTypes": ["order_failed"]
        }
    }`))
    if err != nil {
        t.Fatalf("decodeMessage returned error: %v", err)
    }

    if messageType != contracts.MessageTypeSubscribe {
        t.Fatalf(
            "expected message type %q, got %q",
            contracts.MessageTypeSubscribe,
            messageType,
        )
    }

    subscribe, ok := payload.(*contracts.SubscribeMessage)
    if !ok {
        t.Fatalf("expected *SubscribeMessage, got %T", payload)
    }

    if subscribe.RequestID != "subscription-1" {
        t.Fatalf(
            "expected request ID subscription-1, got %q",
            subscribe.RequestID,
        )
    }

    if len(subscribe.Filters.Services) != 1 ||
        subscribe.Filters.Services[0] != "orders" {
        t.Fatalf("unexpected service filters: %#v", subscribe.Filters.Services)
    }
}

func TestDecodeMessageRejectsMalformedJSON(t *testing.T) {
    _, _, err := decodeMessage([]byte(`{"type":`))

    if !errors.Is(err, ErrInvalidJSON) {
        t.Fatalf("expected ErrInvalidJSON, got %v", err)
    }
}

func TestDecodeMessageRejectsEmptyObject(t *testing.T) {
    _, _, err := decodeMessage([]byte(`{}`))

    if !errors.Is(err, ErrInvalidMessage) {
        t.Fatalf("expected ErrInvalidMessage, got %v", err)
    }
}

func TestDecodeMessageRejectsMissingType(t *testing.T) {
    _, _, err := decodeMessage([]byte(`{
        "client": "fixture-client"
    }`))

    if !errors.Is(err, ErrInvalidMessage) {
        t.Fatalf("expected ErrInvalidMessage, got %v", err)
    }
}

func TestDecodeMessageRejectsInvalidTypeValue(t *testing.T) {
    _, _, err := decodeMessage([]byte(`{
        "type": 123
    }`))

    if !errors.Is(err, ErrInvalidMessage) {
        t.Fatalf("expected ErrInvalidMessage, got %v", err)
    }
}

func TestDecodeMessageRejectsUnsupportedType(t *testing.T) {
    _, _, err := decodeMessage([]byte(`{
        "type": "unknown"
    }`))

    if !errors.Is(err, ErrUnsupportedType) {
        t.Fatalf("expected ErrUnsupportedType, got %v", err)
    }
}

func TestDecodeMessageRejectsUnknownFields(t *testing.T) {
    _, _, err := decodeMessage([]byte(`{
        "type": "hello",
        "mode": "producer",
        "client": "fixture-client",
        "version": "1.0",
        "unexpected": "field"
    }`))

    if !errors.Is(err, ErrInvalidMessage) {
        t.Fatalf("expected ErrInvalidMessage, got %v", err)
    }
}

func TestDecodeMessageRejectsMalformedKnownMessage(t *testing.T) {
    _, _, err := decodeMessage([]byte(`{
        "type": "event",
        "requestId": "request-1",
        "event": "not-an-object"
    }`))

    if !errors.Is(err, ErrInvalidMessage) {
        t.Fatalf("expected ErrInvalidMessage, got %v", err)
    }
}