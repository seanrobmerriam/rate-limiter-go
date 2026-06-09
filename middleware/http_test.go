package middleware

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	ratelimiter "github.com/seanrobmerriam/rate-limiter-go"
	"github.com/stretchr/testify/assert"
)

type mockLimiter struct {
	checkFunc func(ctx context.Context, cfg ratelimiter.Config) (ratelimiter.Result, error)
	resetFunc func(ctx context.Context, key ratelimiter.Key) error
	closeFunc func() error
}

func (m *mockLimiter) Check(ctx context.Context, cfg ratelimiter.Config) (ratelimiter.Result, error) {
	if m.checkFunc != nil {
		return m.checkFunc(ctx, cfg)
	}
	return ratelimiter.Result{}, nil
}

func (m *mockLimiter) Reset(ctx context.Context, key ratelimiter.Key) error {
	if m.resetFunc != nil {
		return m.resetFunc(ctx, key)
	}
	return nil
}

func (m *mockLimiter) ResetMulti(ctx context.Context, keys ...ratelimiter.Key) error {
	return nil
}

func (m *mockLimiter) Wait(ctx context.Context, cfg ratelimiter.Config) error {
	return nil
}

func (m *mockLimiter) Peek(ctx context.Context, cfg ratelimiter.Config) (ratelimiter.State, error) {
	return ratelimiter.State{}, nil
}

func (m *mockLimiter) Close() error {
	if m.closeFunc != nil {
		return m.closeFunc()
	}
	return nil
}

func TestMiddleware_AllowedRequest(t *testing.T) {
	handlerCalled := 0
	nextHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handlerCalled++
		w.WriteHeader(http.StatusOK)
	})

	limiter := &mockLimiter{
		checkFunc: func(ctx context.Context, cfg ratelimiter.Config) (ratelimiter.Result, error) {
			return ratelimiter.Result{
				Status:    ratelimiter.Allowed,
				Remaining: 99,
				ResetAt:   time.Now().Add(time.Second),
			}, nil
		},
	}

	cfg := ratelimiter.Config{
		Algorithm: ratelimiter.TokenBucket,
		Rate:      100,
		BurstSize: 10,
		Window:    time.Second,
	}
	keyFn := func(r *http.Request) ratelimiter.Key {
		return ratelimiter.Key("client-1")
	}

	middleware := Middleware(limiter, cfg, keyFn)
	handler := middleware(nextHandler)

	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code, "expected status OK")
	assert.Equal(t, 1, handlerCalled, "handler should be called exactly once")
	assert.Equal(t, "99", w.Header().Get("X-RateLimit-Remaining"), "X-RateLimit-Remaining header should be present")
	assert.NotEmpty(t, w.Header().Get("X-RateLimit-Reset"), "X-RateLimit-Reset header should be present")
}

func TestMiddleware_DeniedRequest(t *testing.T) {
	handlerCalled := 0
	nextHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handlerCalled++
		w.WriteHeader(http.StatusOK)
	})

	limiter := &mockLimiter{
		checkFunc: func(ctx context.Context, cfg ratelimiter.Config) (ratelimiter.Result, error) {
			return ratelimiter.Result{
				Status:     ratelimiter.Denied,
				Remaining:  0,
				RetryAfter: 30 * time.Second,
				ResetAt:    time.Now().Add(30 * time.Second),
			}, nil
		},
	}

	cfg := ratelimiter.Config{
		Algorithm: ratelimiter.TokenBucket,
		Rate:      100,
		BurstSize: 10,
		Window:    time.Second,
	}
	keyFn := func(r *http.Request) ratelimiter.Key {
		return ratelimiter.Key("client-1")
	}

	middleware := Middleware(limiter, cfg, keyFn)
	handler := middleware(nextHandler)

	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	assert.Equal(t, http.StatusTooManyRequests, w.Code, "expected 429 status")
	assert.Equal(t, 0, handlerCalled, "handler should never be called")
	assert.Equal(t, "30", w.Header().Get("Retry-After"), "Retry-After header should be present")
	assert.Equal(t, "application/json", w.Header().Get("Content-Type"), "Content-Type should be application/json")

	var responseBody map[string]interface{}
	err := json.Unmarshal(w.Body.Bytes(), &responseBody)
	assert.NoError(t, err, "response should be valid JSON")
	assert.Equal(t, "rate limit exceeded", responseBody["error"], "error message should be present")
	assert.Equal(t, float64(30), responseBody["retry_after"], "retry_after should be present")
}

func TestMiddleware_StoreError(t *testing.T) {
	handlerCalled := 0
	nextHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handlerCalled++
		w.WriteHeader(http.StatusOK)
	})

	limiter := &mockLimiter{
		checkFunc: func(ctx context.Context, cfg ratelimiter.Config) (ratelimiter.Result, error) {
			return ratelimiter.Result{}, assert.AnError
		},
	}

	cfg := ratelimiter.Config{
		Algorithm: ratelimiter.TokenBucket,
		Rate:      100,
		BurstSize: 10,
		Window:    time.Second,
	}
	keyFn := func(r *http.Request) ratelimiter.Key {
		return ratelimiter.Key("client-1")
	}

	middleware := Middleware(limiter, cfg, keyFn)
	handler := middleware(nextHandler)

	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	assert.Equal(t, http.StatusServiceUnavailable, w.Code, "expected 503 status")
	assert.Equal(t, 0, handlerCalled, "handler should never be called")
	assert.Equal(t, "application/json", w.Header().Get("Content-Type"), "Content-Type should be application/json")

	var responseBody map[string]string
	err := json.Unmarshal(w.Body.Bytes(), &responseBody)
	assert.NoError(t, err, "response should be valid JSON")
	assert.Equal(t, "rate limiter unavailable", responseBody["error"], "error message should be present")
}

func TestMiddleware_IndependentRateLimitsByKey(t *testing.T) {
	key1HandlerCalls := 0
	key2HandlerCalls := 0

	callCounts := make(map[ratelimiter.Key]int)

	limiter := &mockLimiter{
		checkFunc: func(ctx context.Context, cfg ratelimiter.Config) (ratelimiter.Result, error) {
			callCounts[cfg.Key]++

			if callCounts[cfg.Key] <= 2 {
				return ratelimiter.Result{
					Status:    ratelimiter.Allowed,
					Remaining: 2 - callCounts[cfg.Key],
					ResetAt:   time.Now().Add(time.Second),
				}, nil
			}
			return ratelimiter.Result{
				Status:     ratelimiter.Denied,
				Remaining:  0,
				RetryAfter: 30 * time.Second,
				ResetAt:    time.Now().Add(30 * time.Second),
			}, nil
		},
	}

	cfg := ratelimiter.Config{
		Algorithm: ratelimiter.TokenBucket,
		Rate:      100,
		BurstSize: 10,
		Window:    time.Second,
	}

	keyFn := func(r *http.Request) ratelimiter.Key {
		return ratelimiter.Key(r.Header.Get("X-Client-ID"))
	}

	nextHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Client-ID") == "key1" {
			key1HandlerCalls++
		} else if r.Header.Get("X-Client-ID") == "key2" {
			key2HandlerCalls++
		}
		w.WriteHeader(http.StatusOK)
	})

	middleware := Middleware(limiter, cfg, keyFn)
	handler := middleware(nextHandler)

	for i := 0; i < 3; i++ {
		req1 := httptest.NewRequest(http.MethodGet, "/test", nil)
		req1.Header.Set("X-Client-ID", "key1")
		w1 := httptest.NewRecorder()
		handler.ServeHTTP(w1, req1)
	}

	for i := 0; i < 3; i++ {
		req2 := httptest.NewRequest(http.MethodGet, "/test", nil)
		req2.Header.Set("X-Client-ID", "key2")
		w2 := httptest.NewRecorder()
		handler.ServeHTTP(w2, req2)
	}

	assert.Equal(t, 2, key1HandlerCalls, "key1 handler should be called twice (first 2 requests allowed)")
	assert.Equal(t, 2, key2HandlerCalls, "key2 handler should be called twice (first 2 requests allowed)")
	assert.Equal(t, 3, callCounts[ratelimiter.Key("key1")], "limiter should be called 3 times for key1")
	assert.Equal(t, 3, callCounts[ratelimiter.Key("key2")], "limiter should be called 3 times for key2")
}

func TestMiddleware_Observability(t *testing.T) {
	t.Run("allowed requests emit an allowed event", func(t *testing.T) {
		var (
			mu    sync.Mutex
			event ratelimiter.Event
		)

		limiter := &mockLimiter{
			checkFunc: func(ctx context.Context, cfg ratelimiter.Config) (ratelimiter.Result, error) {
				return ratelimiter.Result{
					Status:    ratelimiter.Allowed,
					Remaining: 4,
					ResetAt:   time.Now().Add(time.Second),
				}, nil
			},
		}

		handler := MiddlewareWithOptions(
			limiter,
			ratelimiter.Config{Algorithm: ratelimiter.TokenBucket, Rate: 5, BurstSize: 5, Window: time.Second},
			func(r *http.Request) ratelimiter.Key { return ratelimiter.Key("allowed-client") },
			WithObserver(ratelimiter.ObserverFunc(func(ctx context.Context, got ratelimiter.Event) {
				mu.Lock()
				defer mu.Unlock()
				event = got
			})),
		)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		}))

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/test", nil)

		handler.ServeHTTP(w, req)

		mu.Lock()
		defer mu.Unlock()
		assert.Equal(t, ratelimiter.EventAllowed, event.Kind)
		assert.Equal(t, ratelimiter.Key("allowed-client"), event.Key)
		assert.Equal(t, ratelimiter.TokenBucket, event.Algorithm)
		assert.Equal(t, ratelimiter.Allowed, event.Status)
		assert.Equal(t, 4, event.Remaining)
		assert.GreaterOrEqual(t, event.Duration, time.Duration(0))
	})

	t.Run("denied requests emit retry metadata", func(t *testing.T) {
		var event ratelimiter.Event

		limiter := &mockLimiter{
			checkFunc: func(ctx context.Context, cfg ratelimiter.Config) (ratelimiter.Result, error) {
				return ratelimiter.Result{
					Status:     ratelimiter.Denied,
					RetryAfter: 3 * time.Second,
					ResetAt:    time.Now().Add(3 * time.Second),
				}, nil
			},
		}

		handler := MiddlewareWithOptions(
			limiter,
			ratelimiter.Config{Algorithm: ratelimiter.SlidingWindow, Rate: 1, Window: time.Second},
			func(r *http.Request) ratelimiter.Key { return ratelimiter.Key("denied-client") },
			WithObserver(ratelimiter.ObserverFunc(func(ctx context.Context, got ratelimiter.Event) {
				event = got
			})),
		)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		}))

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/test", nil)

		handler.ServeHTTP(w, req)

		assert.Equal(t, ratelimiter.EventDenied, event.Kind)
		assert.Equal(t, ratelimiter.Key("denied-client"), event.Key)
		assert.Equal(t, ratelimiter.SlidingWindow, event.Algorithm)
		assert.Equal(t, ratelimiter.Denied, event.Status)
		assert.Equal(t, 3*time.Second, event.RetryAfter)
		assert.False(t, event.ResetAt.IsZero())
	})

	t.Run("limiter errors emit error events", func(t *testing.T) {
		var event ratelimiter.Event

		limiter := &mockLimiter{
			checkFunc: func(ctx context.Context, cfg ratelimiter.Config) (ratelimiter.Result, error) {
				return ratelimiter.Result{}, assert.AnError
			},
		}

		handler := MiddlewareWithOptions(
			limiter,
			ratelimiter.Config{Algorithm: ratelimiter.TokenBucket, Rate: 2, BurstSize: 2, Window: time.Second},
			func(r *http.Request) ratelimiter.Key { return ratelimiter.Key("error-client") },
			WithObserver(ratelimiter.ObserverFunc(func(ctx context.Context, got ratelimiter.Event) {
				event = got
			})),
		)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		}))

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/test", nil)

		handler.ServeHTTP(w, req)

		assert.Equal(t, ratelimiter.EventError, event.Kind)
		assert.Equal(t, ratelimiter.Key("error-client"), event.Key)
		assert.Equal(t, ratelimiter.TokenBucket, event.Algorithm)
		assert.ErrorIs(t, event.Err, assert.AnError)
		assert.GreaterOrEqual(t, event.Duration, time.Duration(0))
	})
}

func TestMiddleware_Counters(t *testing.T) {
	counters := ratelimiter.NewCounters()

	results := []struct {
		name   string
		result ratelimiter.Result
		err    error
	}{
		{
			name: "allowed",
			result: ratelimiter.Result{
				Status:    ratelimiter.Allowed,
				Remaining: 1,
				ResetAt:   time.Now().Add(time.Second),
			},
		},
		{
			name: "denied",
			result: ratelimiter.Result{
				Status:     ratelimiter.Denied,
				RetryAfter: time.Second,
				ResetAt:    time.Now().Add(time.Second),
			},
		},
		{
			name: "error",
			err:  assert.AnError,
		},
	}

	for _, tt := range results {
		t.Run(tt.name, func(t *testing.T) {
			limiter := &mockLimiter{
				checkFunc: func(ctx context.Context, cfg ratelimiter.Config) (ratelimiter.Result, error) {
					return tt.result, tt.err
				},
			}

			handler := MiddlewareWithOptions(
				limiter,
				ratelimiter.Config{Algorithm: ratelimiter.TokenBucket, Rate: 3, BurstSize: 3, Window: time.Second},
				func(r *http.Request) ratelimiter.Key { return ratelimiter.Key(tt.name) },
				WithObserver(counters),
			)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
			}))

			w := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodGet, "/test", nil)
			handler.ServeHTTP(w, req)
		})
	}

	snapshot := counters.Snapshot()
	assert.Equal(t, uint64(1), snapshot.Allowed)
	assert.Equal(t, uint64(1), snapshot.Denied)
	assert.Equal(t, uint64(1), snapshot.Errors)
}

func TestMiddleware_CustomHandlers(t *testing.T) {
	t.Run("custom denied handler overrides default body", func(t *testing.T) {
		limiter := &mockLimiter{
			checkFunc: func(ctx context.Context, cfg ratelimiter.Config) (ratelimiter.Result, error) {
				return ratelimiter.Result{
					Status:     ratelimiter.Denied,
					RetryAfter: 2 * time.Second,
					ResetAt:    time.Now().Add(2 * time.Second),
				}, nil
			},
		}

		handler := MiddlewareWithOptions(
			limiter,
			ratelimiter.Config{Algorithm: ratelimiter.TokenBucket, Rate: 1, BurstSize: 1, Window: time.Second},
			func(r *http.Request) ratelimiter.Key { return ratelimiter.Key("client") },
			WithDeniedHandler(func(w http.ResponseWriter, r *http.Request, result ratelimiter.Result) {
				w.Header().Set("Content-Type", "text/plain")
				w.WriteHeader(http.StatusForbidden)
				_, _ = w.Write([]byte("blocked"))
			}),
		)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		}))

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/test", nil)
		handler.ServeHTTP(w, req)

		assert.Equal(t, http.StatusForbidden, w.Code)
		assert.Equal(t, "text/plain", w.Header().Get("Content-Type"))
		assert.Equal(t, "blocked", w.Body.String())
	})

	t.Run("custom error handler can return caller response", func(t *testing.T) {
		limiter := &mockLimiter{
			checkFunc: func(ctx context.Context, cfg ratelimiter.Config) (ratelimiter.Result, error) {
				return ratelimiter.Result{}, assert.AnError
			},
		}

		handler := MiddlewareWithOptions(
			limiter,
			ratelimiter.Config{Algorithm: ratelimiter.TokenBucket, Rate: 1, BurstSize: 1, Window: time.Second},
			func(r *http.Request) ratelimiter.Key { return ratelimiter.Key("client") },
			WithErrorHandler(func(w http.ResponseWriter, r *http.Request, err error) bool {
				w.WriteHeader(http.StatusBadGateway)
				_, _ = w.Write([]byte("upstream limiter error"))
				return false
			}),
		)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		}))

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/test", nil)
		handler.ServeHTTP(w, req)

		assert.Equal(t, http.StatusBadGateway, w.Code)
		assert.Equal(t, "upstream limiter error", w.Body.String())
	})
}

func TestMiddleware_FailOpen(t *testing.T) {
	handlerCalled := 0
	limiter := &mockLimiter{
		checkFunc: func(ctx context.Context, cfg ratelimiter.Config) (ratelimiter.Result, error) {
			return ratelimiter.Result{}, assert.AnError
		},
	}

	handler := MiddlewareWithOptions(
		limiter,
		ratelimiter.Config{Algorithm: ratelimiter.TokenBucket, Rate: 1, BurstSize: 1, Window: time.Second},
		func(r *http.Request) ratelimiter.Key { return ratelimiter.Key("client") },
		WithFailOpen(),
	)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handlerCalled++
		w.WriteHeader(http.StatusAccepted)
	}))

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	handler.ServeHTTP(w, req)

	assert.Equal(t, 1, handlerCalled)
	assert.Equal(t, http.StatusAccepted, w.Code)
}

func TestMiddleware_StandardHeaders(t *testing.T) {
	t.Run("allowed responses include standard headers", func(t *testing.T) {
		limiter := &mockLimiter{
			checkFunc: func(ctx context.Context, cfg ratelimiter.Config) (ratelimiter.Result, error) {
				return ratelimiter.Result{
					Status:    ratelimiter.Allowed,
					Remaining: 7,
					ResetAt:   time.Now().Add(5 * time.Second),
				}, nil
			},
		}

		handler := MiddlewareWithOptions(
			limiter,
			ratelimiter.Config{Algorithm: ratelimiter.TokenBucket, Rate: 10, BurstSize: 10, Window: 5 * time.Second},
			func(r *http.Request) ratelimiter.Key { return ratelimiter.Key("client") },
			WithStandardHeaders(),
		)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		}))

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/test", nil)
		handler.ServeHTTP(w, req)

		assert.Equal(t, "10", w.Header().Get("RateLimit-Limit"))
		assert.Equal(t, "7", w.Header().Get("RateLimit-Remaining"))
		assert.NotEmpty(t, w.Header().Get("RateLimit-Reset"))
		assert.Equal(t, "7", w.Header().Get("X-RateLimit-Remaining"))
	})

	t.Run("denied responses include standard headers", func(t *testing.T) {
		limiter := &mockLimiter{
			checkFunc: func(ctx context.Context, cfg ratelimiter.Config) (ratelimiter.Result, error) {
				return ratelimiter.Result{
					Status:     ratelimiter.Denied,
					RetryAfter: 4 * time.Second,
					ResetAt:    time.Now().Add(4 * time.Second),
				}, nil
			},
		}

		handler := MiddlewareWithOptions(
			limiter,
			ratelimiter.Config{Algorithm: ratelimiter.SlidingWindow, Rate: 3, Window: 4 * time.Second},
			func(r *http.Request) ratelimiter.Key { return ratelimiter.Key("client") },
			WithStandardHeaders(),
		)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		}))

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/test", nil)
		handler.ServeHTTP(w, req)

		assert.Equal(t, "3", w.Header().Get("RateLimit-Limit"))
		assert.Equal(t, "0", w.Header().Get("RateLimit-Remaining"))
		assert.NotEmpty(t, w.Header().Get("RateLimit-Reset"))
		assert.Equal(t, "4", w.Header().Get("Retry-After"))
	})
}
