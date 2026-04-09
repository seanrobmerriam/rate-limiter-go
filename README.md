# Rate Limiter

A high-performance rate limiting library for Go applications, supporting both token bucket and sliding window algorithms.

## Installation

```bash
go get github.com/seanrobmerriam/rate-limiter-go
```

## Quickstart

```go
package main

import (
    "log/slog"
    "net/http"
    "os"
    "time"

    ratelimiter "github.com/seanrobmerriam/rate-limiter-go"
    "github.com/seanrobmerriam/rate-limiter-go/middleware"
    "github.com/seanrobmerriam/rate-limiter-go/store/memory"
)

func main() {
    // Create a memory store (use store/redis for distributed setups)
    store := memory.New()
    defer store.Close()

    // Create the rate limiter
    limiter := ratelimiter.New(store)

    // Configure rate limiting
    cfg := ratelimiter.Config{
        Key:       "api",                              // Rate limit key
        Algorithm: ratelimiter.TokenBucket,            // or SlidingWindow
        Rate:      100,                                // requests per window
        BurstSize: 50,                                 // burst capacity (token bucket only)
        Window:    time.Second,                        // time window
    }

    // Extract key from request (e.g., API key, IP)
    keyFn := func(r *http.Request) ratelimiter.Key {
        return ratelimiter.Key(r.RemoteAddr) // or extract from header
    }

    // Wrap your handler with rate limiting middleware
    counters := ratelimiter.NewCounters()
    logger := ratelimiter.NewSlogObserver(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

    handler := middleware.MiddlewareWithOptions(limiter, cfg, keyFn,
        middleware.WithObserver(counters),
        middleware.WithObserver(logger),
    )(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        w.WriteHeader(http.StatusOK)
        _, _ = w.Write([]byte("ok"))
    }))

    _ = http.ListenAndServe(":8080", handler)
}
```

## Observability

The middleware can emit structured events through observers. The library includes:

- `ratelimiter.ObserverFunc` for lightweight callbacks
- `ratelimiter.NewCounters()` for in-process allow/deny/error counts
- `ratelimiter.NewSlogObserver(...)` for structured `slog` output

Use `middleware.MiddlewareWithOptions(...)` with `middleware.WithObserver(...)` to attach one or more observers.

## Integration Helpers

The root package includes small helpers for common `net/http` integrations:

- `ratelimiter.HeaderKeyFunc("X-Client-ID")` to derive keys from request headers
- `ratelimiter.RemoteIPKeyFunc()` to derive keys from `RemoteAddr`
- `cfg.WithKey(key)` to clone a shared config safely for per-request use

## Chi Integration

The middleware package includes a Chi adapter and a route-param key helper:

```go
router := chi.NewRouter()
router.Use(middleware.Chi(
    limiter,
    cfg,
    middleware.ChiURLParamKeyFunc("accountID"),
    middleware.WithStandardHeaders(),
))

router.Get("/accounts/{accountID}", func(w http.ResponseWriter, r *http.Request) {
    w.WriteHeader(http.StatusOK)
})
```

This is useful when the rate-limit key comes from Chi route params rather than headers or remote IPs.

## Gin Integration

The middleware package also includes a Gin adapter with a Gin route-param key helper:

```go
router := gin.New()
router.Use(middleware.Gin(
    limiter,
    cfg,
    middleware.GinParamKeyFunc("accountID"),
    middleware.WithStandardHeaders(),
))

router.GET("/accounts/:accountID", func(c *gin.Context) {
    c.Status(http.StatusOK)
})
```

Use this when the rate-limit key should come from Gin route params while keeping the same middleware options and observer behavior as the `net/http` and Chi adapters.

## Demo App

The demo server now shows the full middleware option surface working together:

- observers via `ratelimiter.NewCounters()` and `ratelimiter.NewSlogObserver(...)`
- fail-open behavior with `middleware.WithFailOpen()`
- standard rate-limit headers with `middleware.WithStandardHeaders()`
- a `/metrics` endpoint exposing observer counter totals as JSON

Run it with:

```bash
go run ./cmd/demo
```

Then try:

- `curl -i localhost:8080/api/resource -H 'X-Client-ID: demo-client'`
- `curl localhost:8080/metrics`

## Algorithm Explanation

### Token Bucket

The token bucket algorithm allows burst traffic up to a maximum size while enforcing an average rate over time.

- **Rate**: Number of tokens added per window (e.g., 100 requests/second)
- **BurstSize**: Maximum number of tokens that can be accumulated (allows short bursts)
- **Behavior**: When a request arrives, tokens are consumed if available. Tokens are continuously refilled at the configured rate

Use token bucket when:
- You want to allow occasional bursts
- You need simple, memory-efficient rate limiting
- Approximate rate limiting is acceptable

### Sliding Window

The sliding window algorithm provides precise rate limiting by tracking the exact timestamp of each request within the window.

- **Rate**: Maximum requests allowed within the window
- **Window**: Time window (e.g., 1 second)
- **Behavior**: Maintains a list of request timestamps, removes expired ones, and allows only `rate` requests within the window

Use sliding window when:
- You need precise rate limiting at window boundaries
- You want to avoid the "burst at window edge" problem
- Memory usage is not a concern (stores all request timestamps)

## Configuration Reference

### Config struct

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `Key` | `Key` | Yes | Unique identifier for the rate limit (e.g., user ID, API key, IP) |
| `Algorithm` | `Algorithm` | Yes | Rate limiting algorithm: `token_bucket` or `sliding_window` |
| `Rate` | `int` | Yes | Number of requests allowed per window (must be > 0) |
| `Window` | `time.Duration` | Yes | Time window for rate limiting (must be > 0) |
| `BurstSize` | `int` | No* | Maximum burst capacity for token bucket (must be >= 0) |

*BurstSize is only used with TokenBucket algorithm; set to 0 or omit for sliding window.

### Result struct

| Field | Type | Description |
|-------|------|-------------|
| `Status` | `Status` | `"allowed"` or `"denied"` |
| `Remaining` | `int` | Remaining requests in current window |
| `RetryAfter` | `time.Duration` | Duration until a request will be allowed (if denied) |
| `ResetAt` | `time.Time` | Timestamp when the rate limit window resets |

### Store Options

- **Memory Store** (`store/memory`): In-memory rate limiting, fast but single-instance
- **Redis Store** (`store/redis`): Distributed rate limiting across multiple instances

## Benchmark Results Summary

Based on benchmarks on a modern laptop (Intel Core i3-4010U @ 1.70GHz):

| Benchmark | Operations/sec | ns/op | Allocations |
|-----------|---------------|-------|-------------|
| TokenBucket (memory) | ~4.3M | ~275 | 0 allocs |
| SlidingWindow (memory) | ~390K | ~2800 | 1 alloc |
| TokenBucket Parallel | ~2.3M | ~500 | 0 allocs |
| SlidingWindow Parallel | ~350K | ~3400 | 1 alloc |
| Middleware (allowed) | ~250K | ~4500 | 17 allocs |
| Middleware (denied) | ~250K | ~4500 | 17 allocs |

Key observations:
- Token bucket is ~10x faster than sliding window due to O(1) vs O(n) complexity
- Zero allocations for token bucket checks (highly GC-friendly)
- Middleware overhead is ~4.5μs per request

For detailed benchmark results, see [README_BENCHMARKS.md](README_BENCHMARKS.md).
