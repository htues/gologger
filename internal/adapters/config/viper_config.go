package config

import (
	"strings"

	"github.com/fsnotify/fsnotify"
	"github.com/hftamayo/gologger/internal/domain/config"
	"github.com/spf13/viper"
)

// ViperConfigLoader loads configuration using Viper
type ViperConfigLoader struct {
	viper *viper.Viper
}

// NewViperConfigLoader creates a new Viper configuration loader
func NewViperConfigLoader() *ViperConfigLoader {
	v := viper.New()

	// Set default values
	v.SetDefault("server.port", "8080")
	v.SetDefault("server.host", "0.0.0.0")
	v.SetDefault("server.read_timeout", "15s")
	v.SetDefault("server.write_timeout", "15s")
	v.SetDefault("server.idle_timeout", "60s")

	v.SetDefault("storage.data_dir", "./logs")
	v.SetDefault("storage.rotation_days", 7)
	v.SetDefault("storage.max_file_size", 104857600) // 100MB
	v.SetDefault("storage.buffer_size", 1000)

	v.SetDefault("logging.level", "info")
	v.SetDefault("logging.format", "json")
	v.SetDefault("logging.output_path", "stdout")

	v.SetDefault("rate_limit.enabled", true)
	v.SetDefault("rate_limit.requests_per", 1000)
	v.SetDefault("rate_limit.window", "1m")

	// Environment variable configuration
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	return &ViperConfigLoader{viper: v}
}

// Load loads the configuration
func (vcl *ViperConfigLoader) Load() (*config.Config, error) {
	cfg := &config.Config{}

	if err := vcl.viper.Unmarshal(cfg); err != nil {
		return nil, err
	}

	return cfg, nil
}

// WatchConfig watches for configuration changes
func (vcl *ViperConfigLoader) WatchConfig(onChange func(*config.Config)) {
	vcl.viper.WatchConfig()
	vcl.viper.OnConfigChange(func(e fsnotify.Event) {
		if cfg, err := vcl.Load(); err == nil {
			onChange(cfg)
		}
	})
}
