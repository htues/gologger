package config

import (
	"time"

	"github.com/hftamayo/gologger/internal/contracts"
)

// Config holds all configuration for the logger service
type Config struct {
	Server    ServerConfig    `mapstructure:"server"`
	Storage   StorageConfig   `mapstructure:"storage"`
	Logging   LoggingConfig   `mapstructure:"logging"`
	RateLimit RateLimitConfig `mapstructure:"rate_limit"`
}

// ServerConfig holds server-related configuration
type ServerConfig struct {
	Port           string        `mapstructure:"port"`
	Host           string        `mapstructure:"host"`
	ReadTimeout    time.Duration `mapstructure:"read_timeout"`
	WriteTimeout   time.Duration `mapstructure:"write_timeout"`
	IdleTimeout    time.Duration `mapstructure:"idle_timeout"`
	MaxBodyBytes   int64         `mapstructure:"max_body_bytes"`
	MaxConnections int           `mapstructure:"max_connections"`
}

// StorageConfig holds storage-related configuration
type StorageConfig struct {
	DataDir      string `mapstructure:"data_dir"`
	RotationDays int    `mapstructure:"rotation_days"`
	MaxFileSize  int64  `mapstructure:"max_file_size"`
	BufferSize   int    `mapstructure:"buffer_size"`
}

// LoggingConfig holds logging-related configuration
type LoggingConfig struct {
	Level      contracts.EventLevel `mapstructure:"level"`
	Format     string               `mapstructure:"format"`
	OutputPath string               `mapstructure:"output_path"`
}

// RateLimitConfig holds rate limiting configuration
type RateLimitConfig struct {
	Enabled     bool          `mapstructure:"enabled"`
	RequestsPer int           `mapstructure:"requests_per"`
	Window      time.Duration `mapstructure:"window"`
}

// DefaultConfig returns a default configuration
func DefaultConfig() *Config {
	return &Config{
		Server: ServerConfig{
			Port:           "8080",
			Host:           "0.0.0.0",
			ReadTimeout:    15 * time.Second,
			WriteTimeout:   15 * time.Second,
			IdleTimeout:    60 * time.Second,
			MaxBodyBytes:   1 << 20,
			MaxConnections: 100,
		},
		Storage: StorageConfig{
			DataDir:      "./logs",
			RotationDays: 7,
			MaxFileSize:  100 * 1024 * 1024,
			BufferSize:   1000,
		},
		Logging: LoggingConfig{
			Level:      contracts.EventLevelInfo,
			Format:     "json",
			OutputPath: "stdout",
		},
		RateLimit: RateLimitConfig{
			Enabled:     true,
			RequestsPer: 1000,
			Window:      1 * time.Minute,
		},
	}
}
