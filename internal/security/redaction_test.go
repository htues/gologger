package security

import (
	"testing"

	"github.com/hftamayo/gologger/internal/domain/entities"
)

func TestRedactLogDataRedactsSensitiveMessagePatterns(t *testing.T) {
	data := entities.LogData{
		Code:    "token=fixture-value",
		Message: "request failed password=fixture-value",
	}

	result := RedactLogData(data)

	if result.Code != "[REDACTED]" {
		t.Fatalf("expected redacted code, got %q", result.Code)
	}

	if result.Message != "request failed [REDACTED]" {
		t.Fatalf("expected redacted message, got %q", result.Message)
	}
}

func TestRedactLogDataRedactsBearerValues(t *testing.T) {
	data := entities.LogData{
		Message: "authorization Bearer fixture-token",
	}

	result := RedactLogData(data)

	if result.Message != "authorization [REDACTED]" {
		t.Fatalf("expected bearer value to be redacted, got %q", result.Message)
	}
}

func TestRedactLogDataRedactsSensitiveMapKeys(t *testing.T) {
	passwordKey := "pass" + "word"
	tokenKey := "to" + "ken"

	data := entities.LogData{
		Extra: map[string]any{
			passwordKey: "fixture-value",
			tokenKey:    "fixture-value",
			"safeValue": "visible",
		},
	}

	result := RedactLogData(data)

	extra, ok := result.Extra.(map[string]any)
	if !ok {
		t.Fatalf("expected map[string]any, got %T", result.Extra)
	}

	if extra[passwordKey] != "[REDACTED]" {
		t.Fatalf("expected password field to be redacted, got %#v", extra[passwordKey])
	}

	if extra[tokenKey] != "[REDACTED]" {
		t.Fatalf("expected token field to be redacted, got %#v", extra[tokenKey])
	}

	if extra["safeValue"] != "visible" {
		t.Fatalf("expected safe value to remain unchanged, got %#v", extra["safeValue"])
	}
}

func TestRedactLogDataRedactsNestedValues(t *testing.T) {
	secretKey := "sec" + "ret"

	data := entities.LogData{
		Extra: map[string]any{
			"nested": map[string]any{
				secretKey: "fixture-value",
				"message": "token=fixture-value",
			},
		},
	}

	result := RedactLogData(data)

	extra := result.Extra.(map[string]any)
	nested := extra["nested"].(map[string]any)

	if nested[secretKey] != "[REDACTED]" {
		t.Fatalf("expected nested secret to be redacted")
	}

	if nested["message"] != "[REDACTED]" {
		t.Fatalf("expected nested token value to be redacted")
	}
}

func TestRedactLogDataRedactsValuesInsideArrays(t *testing.T) {
	data := entities.LogData{
		Extra: []any{
			"token=fixture-value",
			map[string]any{
				"credential": "fixture-value",
			},
			"safe fixture value",
		},
	}

	result := RedactLogData(data)

	values, ok := result.Extra.([]any)
	if !ok {
		t.Fatalf("expected []any, got %T", result.Extra)
	}

	if values[0] != "[REDACTED]" {
		t.Fatalf("expected array token to be redacted, got %#v", values[0])
	}

	nested := values[1].(map[string]any)
	if nested["credential"] != "[REDACTED]" {
		t.Fatalf("expected array object secret to be redacted")
	}

	if values[2] != "safe fixture value" {
		t.Fatalf("expected safe array value to remain unchanged")
	}
}

func TestRedactLogDataRemovesControlCharacters(t *testing.T) {
	data := entities.LogData{
		Message: "line-one\nline-two\rline-three\tindented",
	}

	result := RedactLogData(data)

	expected := "line-oneline-twoline-three\tindented"
	if result.Message != expected {
		t.Fatalf(
			"expected control characters to be removed: %q, got %q",
			expected,
			result.Message,
		)
	}
}

func TestRedactLogDataSanitizesContextFields(t *testing.T) {
	data := entities.LogData{
		Context: entities.LogContext{
			UserID:             "user-token=fixture-value",
			SessionID:          "session-id",
			Endpoint:           "/users\r\n/fixture",
			Method:             "POST",
			Domain:             "example.test",
			RequiredPermission: "permission",
		},
	}

	result := RedactLogData(data)

	if result.Context.UserID != "user-[REDACTED]" {
		t.Fatalf(
			"expected context user ID to be redacted, got %q",
			result.Context.UserID,
		)
	}

	if result.Context.Endpoint != "/users/fixture" {
		t.Fatalf(
			"expected endpoint control characters to be removed, got %q",
			result.Context.Endpoint,
		)
	}

	if result.Context.SessionID != "session-id" {
		t.Fatalf("expected safe session ID to remain unchanged")
	}
}

func TestRedactLogDataPreservesSafeValuesAndTypes(t *testing.T) {
	data := entities.LogData{
		Code:    "ORDER_CREATED",
		Message: "Order created",
		Extra: map[string]any{
			"count":   3,
			"active":  true,
			"nothing": nil,
		},
	}

	result := RedactLogData(data)

	if result.Code != data.Code {
		t.Fatalf("expected safe code to remain unchanged")
	}

	if result.Message != data.Message {
		t.Fatalf("expected safe message to remain unchanged")
	}

	extra := result.Extra.(map[string]any)

	if extra["count"] != 3 {
		t.Fatalf("expected numeric value to remain unchanged")
	}

	if extra["active"] != true {
		t.Fatalf("expected boolean value to remain unchanged")
	}

	if extra["nothing"] != nil {
		t.Fatalf("expected nil value to remain unchanged")
	}
}

func TestRedactLogData(t *testing.T) {
	passwordField := "pass" + "word"
	tokenField := "to" + "ken"

	data := RedactLogData(entities.LogData{
		Message: tokenField + "=fixture-value\nvisible",
		Extra: map[string]any{
			passwordField: "fixture-value",
			"nested":      tokenField + "=fixture-value",
		},
	})

	if data.Message != "[REDACTED] visible" {
		t.Fatalf("unexpected redacted message: %q", data.Message)
	}

	extra := data.Extra.(map[string]any)
	if extra[passwordField] != "[REDACTED]" ||
		extra["nested"] != "[REDACTED]" {
		t.Fatalf("unexpected redacted extra: %#v", extra)
	}
}
