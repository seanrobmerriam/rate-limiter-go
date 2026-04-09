package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	ratelimiter "github.com/seanrobmerriam/rate-limiter-go"
	"github.com/seanrobmerriam/rate-limiter-go/middleware"
	"github.com/seanrobmerriam/rate-limiter-go/store/memory"
	"github.com/seanrobmerriam/rate-limiter-go/store/redis"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	redisAddr := getEnv("REDIS_ADDR", "localhost:6379")

	store, backend := initStore(redisAddr, logger)
	logger.Info("store initialized", "backend", backend)

	limiter := ratelimiter.New(store)
	mux, _ := buildMux(limiter, demoConfig())

	srv := &http.Server{
		Addr:    ":8080",
		Handler: mux,
	}

	go func() {
		logger.Info("starting server on :8080")
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("server error", "error", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	logger.Info("shutting down server")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		logger.Error("server forced to shutdown", "error", err)
	}

	if err := limiter.Close(); err != nil {
		logger.Error("error closing limiter", "error", err)
	}

	logger.Info("server exited")
}

func demoConfig() ratelimiter.Config {
	return ratelimiter.Config{
		Algorithm: ratelimiter.TokenBucket,
		Rate:      10,
		BurstSize: 20,
		Window:    time.Second,
	}
}

func buildMux(limiter ratelimiter.Limiter, cfg ratelimiter.Config) (*http.ServeMux, *ratelimiter.Counters) {
	counters := ratelimiter.NewCounters()
	observer := ratelimiter.NewSlogObserver(slog.Default())

	mux := http.NewServeMux()

	mux.HandleFunc("/ping", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("pong"))
	})

	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(counters.Snapshot())
	})

	rateLimitedHandler := middleware.MiddlewareWithOptions(
		limiter,
		cfg,
		extractKey,
		middleware.WithObserver(counters),
		middleware.WithObserver(observer),
		middleware.WithFailOpen(),
		middleware.WithStandardHeaders(),
	)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"message": "resource accessed",
		})
	}))
	mux.Handle("/api/resource", rateLimitedHandler)

	return mux, counters
}

func getEnv(key, defaultValue string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return defaultValue
}

func initStore(redisAddr string, logger *slog.Logger) (ratelimiter.Store, string) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	redisStore, err := redis.New(redisAddr)
	if err != nil {
		logger.Warn("failed to create Redis store", "error", err)
		return memory.New(), "memory"
	}

	if err := redisStore.Ping(ctx); err != nil {
		logger.Warn("Redis unavailable, falling back to in-memory store", "error", err)
		redisStore.Close()
		return memory.New(), "memory"
	}

	return redisStore, "redis"
}

func extractKey(r *http.Request) ratelimiter.Key {
	if clientID := ratelimiter.HeaderKeyFunc("X-Client-ID")(r); clientID != "" {
		return clientID
	}

	return ratelimiter.RemoteIPKeyFunc()(r)
}
