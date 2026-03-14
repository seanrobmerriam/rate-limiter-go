package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ratelimiter/ratelimiter"
	"github.com/ratelimiter/ratelimiter/middleware"
	"github.com/ratelimiter/ratelimiter/store/memory"
	"github.com/ratelimiter/ratelimiter/store/redis"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	redisAddr := getEnv("REDIS_ADDR", "localhost:6379")

	store, backend := initStore(redisAddr, logger)
	logger.Info("store initialized", "backend", backend)

	limiter := ratelimiter.New(store)

	rateLimitCfg := ratelimiter.Config{
		Algorithm: ratelimiter.TokenBucket,
		Rate:      10,
		BurstSize: 20,
		Window:    time.Second,
	}

	mux := http.NewServeMux()

	mux.HandleFunc("/ping", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("pong"))
	})

	rateLimitedHandler := middleware.Middleware(limiter, rateLimitCfg, extractKey)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"message":"resource accessed"}`))
	}))
	mux.Handle("/api/resource", rateLimitedHandler)

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
	if clientID := r.Header.Get("X-Client-ID"); clientID != "" {
		return ratelimiter.Key(clientID)
	}

	ip := r.RemoteAddr
	if colonIdx := len(ip) - 1; colonIdx > 0 {
		for i := colonIdx - 1; i >= 0; i-- {
			if ip[i] == ':' {
				ip = ip[:i]
				break
			}
		}
	}
	return ratelimiter.Key(ip)
}
