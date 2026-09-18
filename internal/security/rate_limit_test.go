package security_test

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestFixedWindowLimiterLimitsEachKeyIndependently(t *testing.T) {
    limiter := NewFixedWindowLimiter(2, time.Minute)

    if !limiter.Allow("client-a") {
        t.Fatal("expected first request to be allowed")
    }

    if !limiter.Allow("client-a") {
        t.Fatal("expected second request to be allowed")
    }

    if limiter.Allow("client-a") {
        t.Fatal("expected third request to be rejected")
    }

    if !limiter.Allow("client-b") {
        t.Fatal("expected a separate key to have its own limit")
    }
}

func TestFixedWindowLimiterResetsAfterWindow(t *testing.T) {
    limiter := NewFixedWindowLimiter(1, 10*time.Millisecond)

    if !limiter.Allow("client-a") {
        t.Fatal("expected first request to be allowed")
    }

    if limiter.Allow("client-a") {
        t.Fatal("expected second request to be rejected")
    }

    time.Sleep(20 * time.Millisecond)

    if !limiter.Allow("client-a") {
        t.Fatal("expected request to be allowed after window reset")
    }
}

func TestFixedWindowLimiterAppliesDefaults(t *testing.T) {
    limiter := NewFixedWindowLimiter(0, 0)

    if !limiter.Allow("client-a") {
        t.Fatal("expected first request to be allowed")
    }

    if limiter.Allow("client-a") {
        t.Fatal("expected default limit to allow only one request")
    }
}

func TestClientKeyUsesAPIKey(t *testing.T) {
    request := httptest.NewRequest(
        http.MethodPost,
        "/logs",
        nil,
    )
    request.RemoteAddr = "192.0.2.10:12345"
    request.Header.Set("X-API-Key", "fixture-api-key")

    if got := ClientKey(request); got != "api-key:fixture-api-key" {
        t.Fatalf("ClientKey() = %q, want %q",
            got,
            "api-key:fixture-api-key",
        )
    }
}

func TestClientKeyUsesIPWhenAPIKeyIsMissing(t *testing.T) {
    request := httptest.NewRequest(
        http.MethodPost,
        "/logs",
        nil,
    )
    request.RemoteAddr = "192.0.2.10:12345"

    if got := ClientKey(request); got != "ip:192.0.2.10" {
        t.Fatalf("ClientKey() = %q, want %q",
            got,
            "ip:192.0.2.10",
        )
    }
}

func TestClientKeyPreservesUnparseableRemoteAddress(t *testing.T) {
    request := httptest.NewRequest(
        http.MethodPost,
        "/logs",
        nil,
    )
    request.RemoteAddr = "fixture-client"

    if got := ClientKey(request); got != "ip:fixture-client" {
        t.Fatalf("ClientKey() = %q, want %q",
            got,
            "ip:fixture-client",
        )
    }
}

func TestMiddlewareAllowsRequest(t *testing.T) {
    limiter := NewFixedWindowLimiter(1, time.Minute)

    next := http.HandlerFunc(func(
        writer http.ResponseWriter,
        _ *http.Request,
    ) {
        writer.WriteHeader(http.StatusNoContent)
    })

    handler := Middleware(limiter, ClientKey, next)

    request := httptest.NewRequest(
        http.MethodGet,
        "/health",
        nil,
    )
    request.RemoteAddr = "192.0.2.10:12345"

    recorder := httptest.NewRecorder()
    handler.ServeHTTP(recorder, request)

    if recorder.Code != http.StatusNoContent {
        t.Fatalf("expected status 204, got %d", recorder.Code)
    }
}

func TestMiddlewareRejectsRateLimitedRequest(t *testing.T) {
    limiter := NewFixedWindowLimiter(1, time.Minute)

    next := http.HandlerFunc(func(
        writer http.ResponseWriter,
        _ *http.Request,
    ) {
        t.Fatal("next handler should not be called")
    })

    handler := Middleware(limiter, ClientKey, next)

    firstRequest := httptest.NewRequest(
        http.MethodGet,
        "/health",
        nil,
    )
    firstRequest.RemoteAddr = "192.0.2.10:12345"

    firstRecorder := httptest.NewRecorder()
    handler.ServeHTTP(firstRecorder, firstRequest)

    secondRequest := httptest.NewRequest(
        http.MethodGet,
        "/health",
        nil,
    )
    secondRequest.RemoteAddr = "192.0.2.10:12345"

    secondRecorder := httptest.NewRecorder()
    handler.ServeHTTP(secondRecorder, secondRequest)

    if secondRecorder.Code != http.StatusTooManyRequests {
        t.Fatalf(
            "expected status 429, got %d",
            secondRecorder.Code,
        )
    }

    if secondRecorder.Header().Get("Retry-After") != "60" {
        t.Fatalf(
            "expected Retry-After header 60, got %q",
            secondRecorder.Header().Get("Retry-After"),
        )
    }
}

func TestMiddlewareAllowsRequestsWithNilLimiter(t *testing.T) {
    called := false

    next := http.HandlerFunc(func(
        writer http.ResponseWriter,
        _ *http.Request,
    ) {
        called = true
        writer.WriteHeader(http.StatusNoContent)
    })

    handler := Middleware(nil, ClientKey, next)

    request := httptest.NewRequest(
        http.MethodGet,
        "/health",
        nil,
    )
    recorder := httptest.NewRecorder()

    handler.ServeHTTP(recorder, request)

    if !called {
        t.Fatal("expected next handler to be called")
    }

    if recorder.Code != http.StatusNoContent {
        t.Fatalf("expected status 204, got %d", recorder.Code)
    }
}

func TestConnectionLimiter(t *testing.T) {
    limiter := NewConnectionLimiter(1)

    if !limiter.TryAcquire() {
        t.Fatal("expected first connection to be acquired")
    }

    if limiter.TryAcquire() {
        t.Fatal("expected second connection to be rejected")
    }

    limiter.Release()

    if !limiter.TryAcquire() {
        t.Fatal("expected released slot to be reusable")
    }

    limiter.Release()
}

func TestNilConnectionLimiter(t *testing.T) {
    var limiter *ConnectionLimiter

    if !limiter.TryAcquire() {
        t.Fatal("expected nil limiter to allow acquisition")
    }

    limiter.Release()
}

func TestNewConnectionLimiterDisablesLimitForNonPositiveValue(t *testing.T) {
    limiter := NewConnectionLimiter(0)

    if !limiter.TryAcquire() {
        t.Fatal("expected nil limiter to allow acquisition")
    }

    limiter.Release()
}

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
