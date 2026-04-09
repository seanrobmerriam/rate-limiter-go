package middleware

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	ratelimiter "github.com/seanrobmerriam/rate-limiter-go"
)

func Chi(limiter ratelimiter.Limiter, cfg ratelimiter.Config, keyFn func(*http.Request) ratelimiter.Key, opts ...Option) func(http.Handler) http.Handler {
	return MiddlewareWithOptions(limiter, cfg, keyFn, opts...)
}

func ChiURLParamKeyFunc(param string) func(*http.Request) ratelimiter.Key {
	return func(r *http.Request) ratelimiter.Key {
		if r == nil {
			return ""
		}
		return ratelimiter.Key(chi.URLParam(r, param))
	}
}