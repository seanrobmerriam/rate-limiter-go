package middleware

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	ratelimiter "github.com/seanrobmerriam/rate-limiter-go"
)

type Option func(*options)

type options struct {
	observers       []ratelimiter.Observer
	deniedHandler   func(http.ResponseWriter, *http.Request, ratelimiter.Result)
	errorHandler    func(http.ResponseWriter, *http.Request, error) bool
	standardHeaders bool
}

func WithObserver(observer ratelimiter.Observer) Option {
	return func(opts *options) {
		if observer != nil {
			opts.observers = append(opts.observers, observer)
		}
	}
}

func WithDeniedHandler(handler func(http.ResponseWriter, *http.Request, ratelimiter.Result)) Option {
	return func(opts *options) {
		opts.deniedHandler = handler
	}
}

func WithErrorHandler(handler func(http.ResponseWriter, *http.Request, error) bool) Option {
	return func(opts *options) {
		opts.errorHandler = handler
	}
}

func WithFailOpen() Option {
	return func(opts *options) {
		opts.errorHandler = func(http.ResponseWriter, *http.Request, error) bool {
			return true
		}
	}
}

func WithStandardHeaders() Option {
	return func(opts *options) {
		opts.standardHeaders = true
	}
}

// Middleware returns an http.Handler that applies rate limiting.
// keyFn extracts the rate limit key from the request (e.g. client IP, API key).
func Middleware(limiter ratelimiter.Limiter, cfg ratelimiter.Config, keyFn func(*http.Request) ratelimiter.Key) func(http.Handler) http.Handler {
	return MiddlewareWithOptions(limiter, cfg, keyFn)
}

// MiddlewareWithOptions returns an http.Handler that applies rate limiting with additive middleware options.
func MiddlewareWithOptions(limiter ratelimiter.Limiter, cfg ratelimiter.Config, keyFn func(*http.Request) ratelimiter.Key, opts ...Option) func(http.Handler) http.Handler {
	config := options{}
	for _, opt := range opts {
		if opt != nil {
			opt(&config)
		}
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			startedAt := time.Now()
			key := keyFn(r)

			cfgCopy := cfg
			cfgCopy.Key = key

			result, err := limiter.Check(r.Context(), cfgCopy)
			if err != nil {
				emitEvent(config.observers, r.Context(), cfgCopy, ratelimiter.Result{}, err, time.Since(startedAt))
				if config.errorHandler != nil {
					if shouldProceed := config.errorHandler(w, r, err); shouldProceed {
						next.ServeHTTP(w, r)
					}
					return
				}
				slog.Error("rate limiter check failed", "error", err)
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusServiceUnavailable)
				json.NewEncoder(w).Encode(map[string]string{
					"error": "rate limiter unavailable",
				})
				return
			}

			switch result.Status {
			case ratelimiter.Allowed:
				emitEvent(config.observers, r.Context(), cfgCopy, result, nil, time.Since(startedAt))
				setStandardHeaders(w, cfgCopy, result, config.standardHeaders)
				w.Header().Set("X-RateLimit-Remaining", fmt.Sprintf("%d", result.Remaining))
				w.Header().Set("X-RateLimit-Reset", fmt.Sprintf("%d", result.ResetAt.Unix()))
				next.ServeHTTP(w, r)

			case ratelimiter.Denied:
				emitEvent(config.observers, r.Context(), cfgCopy, result, nil, time.Since(startedAt))
				setStandardHeaders(w, cfgCopy, result, config.standardHeaders)
				if config.deniedHandler != nil {
					config.deniedHandler(w, r, result)
					return
				}
				retryAfter := int(result.RetryAfter.Seconds())
				w.Header().Set("Content-Type", "application/json")
				w.Header().Set("Retry-After", fmt.Sprintf("%d", retryAfter))
				w.WriteHeader(http.StatusTooManyRequests)
				json.NewEncoder(w).Encode(map[string]interface{}{
					"error":       "rate limit exceeded",
					"retry_after": retryAfter,
				})
			}
		})
	}
}

func setStandardHeaders(w http.ResponseWriter, cfg ratelimiter.Config, result ratelimiter.Result, enabled bool) {
	if !enabled {
		return
	}

	resetSeconds := int(time.Until(result.ResetAt).Seconds())
	if resetSeconds < 0 {
		resetSeconds = 0
	}

	w.Header().Set("RateLimit-Limit", fmt.Sprintf("%d", cfg.Rate))
	w.Header().Set("RateLimit-Remaining", fmt.Sprintf("%d", result.Remaining))
	w.Header().Set("RateLimit-Reset", fmt.Sprintf("%d", resetSeconds))
}

func emitEvent(observers []ratelimiter.Observer, ctx context.Context, cfg ratelimiter.Config, result ratelimiter.Result, err error, duration time.Duration) {
	if len(observers) == 0 {
		return
	}

	event := ratelimiter.Event{
		Key:        cfg.Key,
		Algorithm:  cfg.Algorithm,
		Status:     result.Status,
		Remaining:  result.Remaining,
		RetryAfter: result.RetryAfter,
		ResetAt:    result.ResetAt,
		Err:        err,
		Duration:   duration,
	}

	switch {
	case err != nil:
		event.Kind = ratelimiter.EventError
	case result.Status == ratelimiter.Denied:
		event.Kind = ratelimiter.EventDenied
	default:
		event.Kind = ratelimiter.EventAllowed
	}

	for _, observer := range observers {
		observer.Observe(ctx, event)
	}
}
