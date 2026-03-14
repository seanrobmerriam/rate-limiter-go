package middleware

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"

	"github.com/ratelimiter/ratelimiter"
)

// Middleware returns an http.Handler that applies rate limiting.
// keyFn extracts the rate limit key from the request (e.g. client IP, API key).
func Middleware(limiter ratelimiter.Limiter, cfg ratelimiter.Config, keyFn func(*http.Request) ratelimiter.Key) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			key := keyFn(r)

			cfgCopy := cfg
			cfgCopy.Key = key

			result, err := limiter.Check(r.Context(), cfgCopy)
			if err != nil {
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
				w.Header().Set("X-RateLimit-Remaining", fmt.Sprintf("%d", result.Remaining))
				w.Header().Set("X-RateLimit-Reset", fmt.Sprintf("%d", result.ResetAt.Unix()))
				next.ServeHTTP(w, r)

			case ratelimiter.Denied:
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
