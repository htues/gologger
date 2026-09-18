package security

import (
	"testing"
	"time"

	"github.com/hftamayo/gologger/internal/domain/entities"
)

func TestFixedWindowLimiterIsPerKey(t *testing.T) {
	limiter := NewFixedWindowLimiter(1, time.Minute)
	if !limiter.Allow("one") || limiter.Allow("one") {
		t.Fatal("expected one request per key in the window")
	}
	if !limiter.Allow("two") {
		t.Fatal("expected a separate key to have its own limit")
	}
}

func TestConnectionLimiter(t *testing.T) {
	limiter := NewConnectionLimiter(1)
	if !limiter.TryAcquire() || limiter.TryAcquire() {
		t.Fatal("expected the second connection to be rejected")
	}
	limiter.Release()
	if !limiter.TryAcquire() {
		t.Fatal("expected a released connection slot to be reusable")
	}
	limiter.Release()
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
