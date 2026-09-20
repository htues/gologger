package config

import (
	"testing"
	"time"
)

func TestViperConfigLoaderLoadsDefaults(t *testing.T) {
	loader := NewViperConfigLoader()

	cfg, err := loader.Load()
	if err != nil {
		t.Fatalf("Load() returned error: %v", err)
	}

	if cfg.Server.Port != "8080" {
		t.Errorf("expected port 8080, got %q", cfg.Server.Port)
	}

	if cfg.Server.MaxBodyBytes != 1048576 {
		t.Errorf("expected max body bytes 1048576, got %d", cfg.Server.MaxBodyBytes)
	}

	if cfg.Server.MaxConnections != 100 {
		t.Errorf("expected max connections 100, got %d", cfg.Server.MaxConnections)
	}

	if cfg.Server.ReadTimeout != 15*time.Second {
		t.Errorf("expected read timeout 15s, got %v", cfg.Server.ReadTimeout)
	}

	if !cfg.RateLimit.Enabled {
		t.Error("expected rate limiting to be enabled")
	}

	if cfg.RateLimit.RequestsPer != 1000 {
		t.Errorf("expected 1000 requests per window, got %d", cfg.RateLimit.RequestsPer)
	}

	if cfg.RateLimit.Window != time.Minute {
		t.Errorf("expected rate limit window 1m, got %v", cfg.RateLimit.Window)
	}
}

func TestViperConfigLoaderLoadsEnvironmentOverrides(t *testing.T) {
	t.Setenv("SERVER_PORT", "9090")
	t.Setenv("SERVER_MAX_CONNECTIONS", "25")
	t.Setenv("RATE_LIMIT_ENABLED", "false")
	t.Setenv("RATE_LIMIT_REQUESTS_PER", "50")
	t.Setenv("RATE_LIMIT_WINDOW", "10s")

	loader := NewViperConfigLoader()

	cfg, err := loader.Load()
	if err != nil {
		t.Fatalf("Load() returned error: %v", err)
	}

	if cfg.Server.Port != "9090" {
		t.Errorf("expected port 9090, got %q", cfg.Server.Port)
	}

	if cfg.Server.MaxConnections != 25 {
		t.Errorf("expected max connections 25, got %d", cfg.Server.MaxConnections)
	}

	if cfg.RateLimit.Enabled {
		t.Error("expected rate limiting to be disabled")
	}

	if cfg.RateLimit.RequestsPer != 50 {
		t.Errorf("expected 50 requests per window, got %d", cfg.RateLimit.RequestsPer)
	}

	if cfg.RateLimit.Window != 10*time.Second {
		t.Errorf("expected rate limit window 10s, got %v", cfg.RateLimit.Window)
	}
}
