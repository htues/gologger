package main

import (
	"context"
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
	"github.com/rs/cors"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func main() {
	// Initialize logger
	logger := initLogger()
	defer logger.Sync()

	logger.Info("Starting Logger Service")

	// Load configuration
	configLoader := config.NewViperConfigLoader()
	cfg, err := configLoader.Load()
	if err != nil {
		logger.Fatal("Failed to load configuration", zap.Error(err))
	}

	logger.Info("Configuration loaded",
		zap.String("server.port", cfg.Server.Port),
		zap.String("storage.data_dir", cfg.Storage.DataDir))

	// Initialize storage
	storageAdapter, err := storage.NewJSONFileStorage(
		cfg.Storage.DataDir,
		cfg.Storage.RotationDays,
		cfg.Storage.MaxFileSize,
		logger,
	)
	if err != nil {
		logger.Fatal("Failed to initialize storage", zap.Error(err))
	}
	defer storageAdapter.Close()

	// Initialize logger service
	loggerService := services.NewLoggerService(storageAdapter, logger, &cfg.Logging.Level)

	// Initialize HTTP handler
	handler := http.NewHandler(
		loggerService,
		storageAdapter,
		logger,
	)
	handler.SetConnectionLimiter(security.NewConnectionLimiter(cfg.Server.MaxConnections))

	// Setup router
	router := mux.NewRouter()
	handler.RegisterRoutes(router)

	// Setup CORS
	corsMiddleware := cors.New(cors.Options{
		AllowedOrigins: []string{"*"},
		AllowedMethods: []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"},
		AllowedHeaders: []string{"*"},
	})

	// Create server
	var protectedHandler http.Handler = corsMiddleware.Handler(http.MaxBytesHandler(router, cfg.Server.MaxBodyBytes))
	if cfg.RateLimit.Enabled {
		protectedHandler = security.Middleware(
			security.NewFixedWindowLimiter(cfg.RateLimit.RequestsPer, cfg.RateLimit.Window),
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

	// Start server in a goroutine
	go func() {
		logger.Info("Starting HTTP server", zap.String("address", server.Addr))
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Fatal("Failed to start server", zap.Error(err))
		}
	}()

	// Wait for interrupt signal to gracefully shutdown the server
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	logger.Info("Shutting down server...")

	// Create a deadline for server shutdown
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Attempt graceful shutdown
	if err := server.Shutdown(ctx); err != nil {
		logger.Error("Server forced to shutdown", zap.Error(err))
	}

	logger.Info("Server exited")
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
