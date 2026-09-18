package security

import (
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

type Limiter interface {
	Allow(key string) bool
}

type windowCounter struct {
	started time.Time
	count   int
}

// FixedWindowLimiter is local to one service instance. A shared implementation
// can satisfy Limiter when limits must span multiple replicas.
type FixedWindowLimiter struct {
	limit  int
	window time.Duration
	mu     sync.Mutex
	items  map[string]windowCounter
}

func NewFixedWindowLimiter(limit int, window time.Duration) *FixedWindowLimiter {
	if limit <= 0 {
		limit = 1
	}
	if window <= 0 {
		window = time.Minute
	}
	return &FixedWindowLimiter{limit: limit, window: window, items: make(map[string]windowCounter)}
}

func (limiter *FixedWindowLimiter) Allow(key string) bool {
	now := time.Now()
	limiter.mu.Lock()
	defer limiter.mu.Unlock()
	item := limiter.items[key]
	if item.started.IsZero() || now.Sub(item.started) >= limiter.window {
		limiter.items[key] = windowCounter{started: now, count: 1}
		return true
	}
	if item.count >= limiter.limit {
		return false
	}
	item.count++
	limiter.items[key] = item
	return true
}

func ClientKey(request *http.Request) string {
	if apiKey := strings.TrimSpace(request.Header.Get("X-API-Key")); apiKey != "" {
		return "api-key:" + apiKey
	}
	host, _, err := net.SplitHostPort(request.RemoteAddr)
	if err == nil {
		return "ip:" + host
	}
	return "ip:" + request.RemoteAddr
}

func Middleware(limiter Limiter, key func(*http.Request) string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if limiter != nil && !limiter.Allow(key(request)) {
			writer.Header().Set("Retry-After", "60")
			http.Error(writer, "rate limit exceeded", http.StatusTooManyRequests)
			return
		}
		next.ServeHTTP(writer, request)
	})
}

type ConnectionLimiter struct {
	semaphore chan struct{}
}

func NewConnectionLimiter(limit int) *ConnectionLimiter {
	if limit <= 0 {
		return nil
	}
	return &ConnectionLimiter{semaphore: make(chan struct{}, limit)}
}

func (limiter *ConnectionLimiter) TryAcquire() bool {
	if limiter == nil {
		return true
	}
	select {
	case limiter.semaphore <- struct{}{}:
		return true
	default:
		return false
	}
}

func (limiter *ConnectionLimiter) Release() {
	if limiter != nil {
		<-limiter.semaphore
	}
}
