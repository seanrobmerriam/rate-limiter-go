package ratelimiter_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	ratelimiter "github.com/seanrobmerriam/rate-limiter-go"
	"github.com/seanrobmerriam/rate-limiter-go/middleware"
	"github.com/seanrobmerriam/rate-limiter-go/store/memory"
)

func BenchmarkTokenBucket_MemoryStore(b *testing.B) {
	store := memory.New()
	defer store.Close()

	limiter := ratelimiter.New(store)
	ctx := context.Background()

	cfg := ratelimiter.Config{
		Key:       "benchmark-key",
		Algorithm: ratelimiter.TokenBucket,
		Rate:      100,
		BurstSize: 50,
		Window:    time.Second,
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, _ = limiter.Check(ctx, cfg)
	}
}

func BenchmarkSlidingWindow_MemoryStore(b *testing.B) {
	store := memory.New()
	defer store.Close()

	limiter := ratelimiter.New(store)
	ctx := context.Background()

	cfg := ratelimiter.Config{
		Key:       "benchmark-key",
		Algorithm: ratelimiter.SlidingWindow,
		Rate:      100,
		Window:    time.Second,
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, _ = limiter.Check(ctx, cfg)
	}
}

func BenchmarkTokenBucket_MemoryStore_Parallel(b *testing.B) {
	store := memory.New()
	defer store.Close()

	limiter := ratelimiter.New(store)
	ctx := context.Background()

	cfg := ratelimiter.Config{
		Algorithm: ratelimiter.TokenBucket,
		Rate:      100,
		BurstSize: 50,
		Window:    time.Second,
	}

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			cfg.Key = "benchmark-key"
			_, _ = limiter.Check(ctx, cfg)
		}
	})
}

func BenchmarkSlidingWindow_MemoryStore_Parallel(b *testing.B) {
	store := memory.New()
	defer store.Close()

	limiter := ratelimiter.New(store)
	ctx := context.Background()

	cfg := ratelimiter.Config{
		Algorithm: ratelimiter.SlidingWindow,
		Rate:      100,
		Window:    time.Second,
	}

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			cfg.Key = "benchmark-key"
			_, _ = limiter.Check(ctx, cfg)
		}
	})
}

func BenchmarkMiddleware_Allowed(b *testing.B) {
	store := memory.New()
	defer store.Close()

	limiter := ratelimiter.New(store)

	cfg := ratelimiter.Config{
		Algorithm: ratelimiter.TokenBucket,
		Rate:      1000,
		BurstSize: 1000,
		Window:    time.Second,
	}

	keyFn := func(r *http.Request) ratelimiter.Key {
		return ratelimiter.Key("benchmark-key")
	}

	handler := middleware.Middleware(limiter, cfg, keyFn)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/", nil)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, req)
	}
}

func BenchmarkMiddleware_Denied(b *testing.B) {
	store := memory.New()
	defer store.Close()

	limiter := ratelimiter.New(store)

	cfg := ratelimiter.Config{
		Algorithm: ratelimiter.TokenBucket,
		Rate:      1,
		BurstSize: 1,
		Window:    time.Second,
	}

	keyFn := func(r *http.Request) ratelimiter.Key {
		return ratelimiter.Key("benchmark-key-denied")
	}

	handler := middleware.Middleware(limiter, cfg, keyFn)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/", nil)

	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, req)
	}
}
