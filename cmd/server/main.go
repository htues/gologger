package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gorilla/mux"
	"github.com/hftamayo/gologger/internal/adapters/config"
	"github.com/hftamayo/gologger/internal/adapters/storage"
	"github.com/hftamayo/gologger/internal/domain/services"
	"github.com/hftamayo/gologger/internal/security"
	"github.com/hftamayo/gologger/internal/websockets"
	"github.com/rs/cors"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func main() {
	logger := initLogger()
	defer logger.Sync()

	logger.Info("Starting Logger Service")

	configLoader := config.NewViperConfigLoader()
	cfg, err := configLoader.Load()
	if err != nil {
		logger.Fatal("Failed to load configuration", zap.Error(err))
	}

	logger.Info("Configuration loaded",
		zap.String("server.port", cfg.Server.Port),
		zap.String("storage.data_dir", cfg.Storage.DataDir),
	)

	storageAdapter, err := storage.NewJSONFileStorage(
		cfg.Storage.DataDir,
		cfg.Storage.MaxFileSize,
		cfg.Storage.RotationDays,
		logger,
	)
	if err != nil {
		logger.Fatal("Failed to initialize storage", zap.Error(err))
	}
	defer storageAdapter.Close()

	eventProcessor := services.EventProcessorService{}

	websocketHandler := websockets.NewHandler(
		storageAdapter,
		eventProcessor,
		os.Getenv("LOGGER_API_KEY"),
	)

	// Allow localhost without an API key only for local development.
	if websocketHandler.APIKey == "" {
		websocketHandler.AllowLocalhost = true
	}

	router := mux.NewRouter()
	router.Handle("/ws/events", websocketHandler).Methods(http.MethodGet)
	router.HandleFunc("/health", healthHandler(storageAdapter)).Methods(http.MethodGet)
	router.HandleFunc("/health/live", livenessHandler).Methods(http.MethodGet)
	router.HandleFunc("/health/ready", readinessHandler(storageAdapter)).Methods(http.MethodGet)

	corsMiddleware := cors.New(cors.Options{
		AllowedOrigins: []string{"*"},
		AllowedMethods: []string{
			http.MethodGet,
			http.MethodPost,
			http.MethodPut,
			http.MethodDelete,
			http.MethodOptions,
		},
		AllowedHeaders: []string{"*"},
	})

	var protectedHandler http.Handler = corsMiddleware.Handler(
		http.MaxBytesHandler(router, cfg.Server.MaxBodyBytes),
	)

	if cfg.RateLimit.Enabled {
		protectedHandler = security.Middleware(
			security.NewFixedWindowLimiter(
				cfg.RateLimit.RequestsPer,
				cfg.RateLimit.Window,
			),
			security.ClientKey,
			protectedHandler,
		)
	}

	server := &http.Server{
		Addr:         fmt.Sprintf("%s:%s", cfg.Server.Host, cfg.Server.Port),
		Handler:      protectedHandler,
		ReadTimeout:  cfg.Server.ReadTimeout,
		WriteTimeout: cfg.Server.WriteTimeout,
		IdleTimeout:  cfg.Server.IdleTimeout,
	}

	go func() {
		logger.Info("Starting HTTP server", zap.String("address", server.Addr))
		if err := server.ListenAndServe(); err != nil &&
			err != http.ErrServerClosed {
			logger.Fatal("Failed to start server", zap.Error(err))
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	logger.Info("Shutting down server...")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		logger.Error("Server forced to shutdown", zap.Error(err))
	}

	logger.Info("Server exited")
}

func healthHandler(store interface {
	Health(context.Context) error
}) http.HandlerFunc {
	return func(writer http.ResponseWriter, request *http.Request) {
		if err := store.Health(request.Context()); err != nil {
			writeJSON(writer, http.StatusServiceUnavailable, map[string]any{
				"status": "unhealthy",
				"error":  "storage unavailable",
			})
			return
		}

		writeJSON(writer, http.StatusOK, map[string]any{
			"status":    "healthy",
			"timestamp": time.Now().UTC(),
			"service":   "logger-service",
		})
	}
}

func livenessHandler(writer http.ResponseWriter, _ *http.Request) {
	writeJSON(writer, http.StatusOK, map[string]any{
		"status":    "alive",
		"timestamp": time.Now().UTC(),
	})
}

func readinessHandler(store interface {
	Health(context.Context) error
}) http.HandlerFunc {
	return func(writer http.ResponseWriter, request *http.Request) {
		if err := store.Health(request.Context()); err != nil {
			writeJSON(writer, http.StatusServiceUnavailable, map[string]any{
				"status": "not_ready",
				"error":  "storage unavailable",
			})
			return
		}

		writeJSON(writer, http.StatusOK, map[string]any{
			"status":    "ready",
			"timestamp": time.Now().UTC(),
		})
	}
}

func writeJSON(writer http.ResponseWriter, status int, value any) {
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(status)

	_ = json.NewEncoder(writer).Encode(value)
}

// initLogger initializes the Zap logger
func initLogger() *zap.Logger {
	config := zap.NewProductionConfig()
	config.EncoderConfig.TimeKey = "timestamp"
	config.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	config.EncoderConfig.StacktraceKey = ""

	logger, err := config.Build()
	if err != nil {
		panic(fmt.Sprintf("Failed to initialize logger: %v", err))
	}

	return logger
}
