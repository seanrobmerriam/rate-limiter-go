package middleware

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	ratelimiter "github.com/seanrobmerriam/rate-limiter-go"
	"github.com/stretchr/testify/assert"
)

func TestChi_UsesMiddlewareOptions(t *testing.T) {
	limiter := &mockLimiter{
		checkFunc: func(ctx context.Context, cfg ratelimiter.Config) (ratelimiter.Result, error) {
			return ratelimiter.Result{
				Status:    ratelimiter.Allowed,
				Remaining: 2,
				ResetAt:   time.Now().Add(2 * time.Second),
			}, nil
		},
	}

	router := chi.NewRouter()
	router.Use(Chi(
		limiter,
		ratelimiter.Config{Algorithm: ratelimiter.TokenBucket, Rate: 3, BurstSize: 3, Window: 2 * time.Second},
		ChiURLParamKeyFunc("accountID"),
		WithStandardHeaders(),
	))
	router.Get("/accounts/{accountID}", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	req := httptest.NewRequest(http.MethodGet, "/accounts/abc123", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "2", w.Header().Get("X-RateLimit-Remaining"))
	assert.Equal(t, "3", w.Header().Get("RateLimit-Limit"))
	assert.Equal(t, "2", w.Header().Get("RateLimit-Remaining"))
	assert.Equal(t, "ok", w.Body.String())
}

func TestChiURLParamKeyFunc(t *testing.T) {
	router := chi.NewRouter()
	var got ratelimiter.Key

	router.Get("/teams/{teamID}", func(w http.ResponseWriter, r *http.Request) {
		got = ChiURLParamKeyFunc("teamID")(r)
		w.WriteHeader(http.StatusNoContent)
	})

	req := httptest.NewRequest(http.MethodGet, "/teams/red", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, ratelimiter.Key("red"), got)
}
