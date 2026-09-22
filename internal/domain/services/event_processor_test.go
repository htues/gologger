package services_test

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/hftamayo/gologger/internal/contracts"
	"github.com/hftamayo/gologger/internal/domain/services"
)

func TestProcessEventEnrichesAndMapsEvent(t *testing.T) {
	now := time.Date(
		2026,
		time.September,
		14,
		12,
		30,
		0,
		0,
		time.FixedZone("UTC-5", -5*60*60),
	)

	event := contracts.Event{
		Level:     contracts.EventLevelInformation,
		Service:   " payments ",
		EventType: " payment_declined ",
		Message:   " provider rejected request \n",
		Context: map[string]any{
			"requestPath": "/checkout\n",
		},
		Metadata: map[string]any{
			"orderId": " order-123 ",
		},
	}

	result, err := services.ProcessEvent(event, func() time.Time {
		return now
	})
	if err != nil {
		t.Fatalf("ProcessEvent returned error: %v", err)
	}

	if result.EventID == "" {
		t.Fatal("expected an event ID to be generated")
	}

	if result.Level != contracts.EventLevelInfo {
		t.Fatalf("expected info level, got %q", result.Level)
	}

	if result.Service != "payments" {
		t.Fatalf("expected sanitized service, got %q", result.Service)
	}

	if result.EventType != "payment_declined" {
		t.Fatalf("expected sanitized event type, got %q", result.EventType)
	}

	if result.Message != "provider rejected request" {
		t.Fatalf("expected sanitized message, got %q", result.Message)
	}

	expectedTimestamp := now.UTC()
	if !result.Timestamp.Equal(expectedTimestamp) {
		t.Fatalf(
			"expected timestamp %v, got %v",
			expectedTimestamp,
			result.Timestamp,
		)
	}

	if result.Context["requestPath"] != "/checkout" {
		t.Fatalf("context was not sanitized: %#v", result.Context)
	}

	if result.Metadata["orderId"] != "order-123" {
		t.Fatalf("metadata was not sanitized: %#v", result.Metadata)
	}
}

func TestProcessEventMapsWarningLevel(t *testing.T) {
	event := validEvent()
	event.Level = contracts.EventLevelWarning

	result, err := services.ProcessEvent(event, nil)
	if err != nil {
		t.Fatalf("ProcessEvent returned error: %v", err)
	}

	if result.Level != contracts.EventLevelWarn {
		t.Fatalf("expected warn level, got %q", result.Level)
	}
}

func TestProcessEventRejectsInvalidEvents(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*contracts.Event)
		target error
	}{
		{
			name: "missing level",
			mutate: func(event *contracts.Event) {
				event.Level = ""
			},
			target: services.ErrMissingRequiredField,
		},
		{
			name: "missing service",
			mutate: func(event *contracts.Event) {
				event.Service = ""
			},
			target: services.ErrMissingRequiredField,
		},
		{
			name: "missing event type",
			mutate: func(event *contracts.Event) {
				event.EventType = ""
			},
			target: services.ErrMissingRequiredField,
		},
		{
			name: "missing message",
			mutate: func(event *contracts.Event) {
				event.Message = ""
			},
			target: services.ErrMissingRequiredField,
		},
		{
			name: "unsupported level",
			mutate: func(event *contracts.Event) {
				event.Level = contracts.EventLevel("off")
			},
			target: services.ErrUnsupportedEventLevel,
		},
		{
			name: "sensitive context field",
			mutate: func(event *contracts.Event) {
				event.Context = map[string]any{
					"authorization": "Bearer secret",
				}
			},
			target: services.ErrSensitiveField,
		},
		{
			name: "sensitive metadata field",
			mutate: func(event *contracts.Event) {
				event.Metadata = map[string]any{
					"password": "secret",
				}
			},
			target: services.ErrSensitiveField,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			event := validEvent()
			test.mutate(&event)

			_, err := services.ProcessEvent(event, nil)
			if err == nil {
				t.Fatal("expected an error")
			}

			if !errors.Is(err, test.target) {
				t.Fatalf(
					"expected error wrapping %v, got %v",
					test.target,
					err,
				)
			}
		})
	}
}

func TestProcessEventRejectsOversizedMetadata(t *testing.T) {
	event := validEvent()
	event.Metadata = map[string]any{
		"largeValue": strings.Repeat("x", 70*1024),
	}

	_, err := services.ProcessEvent(event, nil)
	if !errors.Is(err, services.ErrMetadataTooLarge) {
		t.Fatalf("expected metadata size error, got %v", err)
	}
}

func TestProcessEventRejectsInvalidTimestamp(t *testing.T) {
	event := validEvent()
	event.Timestamp = time.Date(
		1999,
		time.December,
		31,
		23,
		59,
		59,
		0,
		time.UTC,
	)

	_, err := services.ProcessEvent(event, nil)
	if err == nil {
		t.Fatal("expected invalid timestamp error")
	}

	if !errors.Is(err, services.ErrInvalidEvent) {
		t.Fatalf("expected invalid event error, got %v", err)
	}
}

func validEvent() contracts.Event {
	return contracts.Event{
		EventID:   "evt-123",
		Level:     contracts.EventLevelInfo,
		Service:   "payments",
		EventType: "payment_declined",
		Message:   "Payment was declined",
	}
}
