package security

import (
	"testing"

	"github.com/hftamayo/gologger/internal/contracts"
)

func TestRedactEventRedactsSensitiveMessagePatterns(t *testing.T) {
	event := contracts.Event{
		Code:    "token=fixture-value",
		Message: "request failed password=fixture-value",
	}

	result := RedactEvent(event)

	if result.Code != redactedValue {
		t.Fatalf("expected redacted code, got %q", result.Code)
	}

	if result.Message != "request failed [REDACTED]" {
		t.Fatalf("expected redacted message, got %q", result.Message)
	}
}

func TestRedactEventRedactsBearerValues(t *testing.T) {
	event := contracts.Event{
		Message: "authorization Bearer fixture-token",
	}

	result := RedactEvent(event)

	if result.Message != "authorization [REDACTED]" {
		t.Fatalf("expected bearer value to be redacted, got %q", result.Message)
	}
}

func TestRedactEventRedactsSensitiveMapKeys(t *testing.T) {
	passwordKey := "pass" + "word"
	tokenKey := "to" + "ken"

	event := contracts.Event{
		Metadata: map[string]any{
			passwordKey: "fixture-value",
			tokenKey:    "fixture-value",
			"safeValue": "visible",
		},
	}

	result := RedactEvent(event)

	if result.Metadata[passwordKey] != redactedValue {
		t.Fatalf("expected password field to be redacted, got %#v", result.Metadata[passwordKey])
	}

	if result.Metadata[tokenKey] != redactedValue {
		t.Fatalf("expected token field to be redacted, got %#v", result.Metadata[tokenKey])
	}

	if result.Metadata["safeValue"] != "visible" {
		t.Fatalf("expected safe value to remain unchanged, got %#v", result.Metadata["safeValue"])
	}
}

func TestRedactEventRedactsNestedValues(t *testing.T) {
	secretKey := "sec" + "ret"

	event := contracts.Event{
		Context: map[string]any{
			"nested": map[string]any{
				secretKey: "fixture-value",
				"message": "token=fixture-value",
			},
		},
	}

	result := RedactEvent(event)

	nested := result.Context["nested"].(map[string]any)

	if nested[secretKey] != redactedValue {
		t.Fatalf("expected nested secret to be redacted")
	}

	if nested["message"] != redactedValue {
		t.Fatalf("expected nested token value to be redacted")
	}
}

func TestRedactEventRedactsValuesInsideArrays(t *testing.T) {
	event := contracts.Event{
		Metadata: map[string]any{
			"values": []any{
				"token=fixture-value",
				map[string]any{
					"credential": "fixture-value",
				},
				"safe fixture value",
			},
		},
	}

	result := RedactEvent(event)

	values, ok := result.Metadata["values"].([]any)
	if !ok {
		t.Fatalf("expected []any, got %T", result.Metadata["values"])
	}

	if values[0] != redactedValue {
		t.Fatalf("expected array token to be redacted, got %#v", values[0])
	}

	nested := values[1].(map[string]any)
	if nested["credential"] != redactedValue {
		t.Fatalf("expected array object secret to be redacted")
	}

	if values[2] != "safe fixture value" {
		t.Fatalf("expected safe array value to remain unchanged")
	}
}

func TestRedactEventRemovesControlCharacters(t *testing.T) {
	event := contracts.Event{
		Message: "line-one\nline-two\rline-three\tindented",
	}

	result := RedactEvent(event)

	expected := "line-oneline-twoline-three\tindented"
	if result.Message != expected {
		t.Fatalf(
			"expected control characters to be removed: %q, got %q",
			expected,
			result.Message,
		)
	}
}

func TestRedactEventSanitizesEventFields(t *testing.T) {
	event := contracts.Event{
		UserID:    "user-token=fixture-value",
		SessionID: "session-id",
		Service:   "orders\r\n-service",
		EventType: "order.created",
		Message:   "Order created",
	}

	result := RedactEvent(event)

	if result.UserID != "user-[REDACTED]" {
		t.Fatalf(
			"expected user ID to be redacted, got %q",
			result.UserID,
		)
	}

	if result.Service != "orders-service" {
		t.Fatalf(
			"expected service control characters to be removed, got %q",
			result.Service,
		)
	}

	if result.SessionID != "session-id" {
		t.Fatalf("expected safe session ID to remain unchanged")
	}
}

func TestRedactEventPreservesSafeValuesAndTypes(t *testing.T) {
	event := contracts.Event{
		Code:    "ORDER_CREATED",
		Message: "Order created",
		Metadata: map[string]any{
			"count":   3,
			"active":  true,
			"nothing": nil,
		},
	}

	result := RedactEvent(event)

	if result.Code != event.Code {
		t.Fatalf("expected safe code to remain unchanged")
	}

	if result.Message != event.Message {
		t.Fatalf("expected safe message to remain unchanged")
	}

	if result.Metadata["count"] != 3 {
		t.Fatalf("expected numeric value to remain unchanged")
	}

	if result.Metadata["active"] != true {
		t.Fatalf("expected boolean value to remain unchanged")
	}

	if result.Metadata["nothing"] != nil {
		t.Fatalf("expected nil value to remain unchanged")
	}
}

func TestRedactEvent(t *testing.T) {
	passwordField := "pass" + "word"
	tokenField := "to" + "ken"

	event := RedactEvent(contracts.Event{
		Message: tokenField + "=fixture-value\nvisible",
		Metadata: map[string]any{
			passwordField: "fixture-value",
			"nested":      tokenField + "=fixture-value",
		},
	})

	if event.Message != "[REDACTED]visible" {
		t.Fatalf("unexpected redacted message: %q", event.Message)
	}

	if event.Metadata[passwordField] != redactedValue ||
		event.Metadata["nested"] != redactedValue {
		t.Fatalf("unexpected redacted metadata: %#v", event.Metadata)
	}
}
