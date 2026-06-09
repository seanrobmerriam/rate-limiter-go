package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	ratelimiter "github.com/seanrobmerriam/rate-limiter-go"
	"github.com/stretchr/testify/assert"
)

type fakeStore struct {
	tokenBucketCheck func(ctx context.Context, key ratelimiter.Key, rate int, burst int, window time.Duration) (ratelimiter.Result, error)
}

func (s *fakeStore) TokenBucketCheck(ctx context.Context, key ratelimiter.Key, rate int, burst int, window time.Duration) (ratelimiter.Result, error) {
	if s.tokenBucketCheck != nil {
		return s.tokenBucketCheck(ctx, key, rate, burst, window)
	}
	return ratelimiter.Result{}, nil
}

func (s *fakeStore) SlidingWindowCheck(ctx context.Context, key ratelimiter.Key, rate int, window time.Duration) (ratelimiter.Result, error) {
	return ratelimiter.Result{}, nil
}

func (s *fakeStore) Check(ctx context.Context, cfg ratelimiter.Config) (ratelimiter.Result, error) {
	switch cfg.Algorithm {
	case ratelimiter.TokenBucket:
		return s.TokenBucketCheck(ctx, cfg.Key, cfg.Rate, cfg.BurstSize, cfg.Window)
	case ratelimiter.SlidingWindow:
		return s.SlidingWindowCheck(ctx, cfg.Key, cfg.Rate, cfg.Window)
	default:
		return ratelimiter.Result{}, nil
	}
}

func (s *fakeStore) Peek(ctx context.Context, cfg ratelimiter.Config) (ratelimiter.State, error) {
	return ratelimiter.State{}, nil
}

func (s *fakeStore) Reset(ctx context.Context, key ratelimiter.Key) error           { return nil }
func (s *fakeStore) ResetMulti(ctx context.Context, keys ...ratelimiter.Key) error  { return nil }
func (s *fakeStore) Ping(ctx context.Context) error                                 { return nil }
func (s *fakeStore) Close() error                                                   { return nil }

func TestBuildMux_ExposesMetricsAndStandardHeaders(t *testing.T) {
	store := &fakeStore{
		tokenBucketCheck: func(ctx context.Context, key ratelimiter.Key, rate int, burst int, window time.Duration) (ratelimiter.Result, error) {
			return ratelimiter.Result{
				Status:    ratelimiter.Allowed,
				Remaining: 9,
				ResetAt:   time.Now().Add(time.Second),
			}, nil
		},
	}

	handler, counters := buildMux(ratelimiter.New(store), demoConfig())
	assert.NotNil(t, counters)

	resourceReq := httptest.NewRequest(http.MethodGet, "/api/resource", nil)
	resourceReq.Header.Set("X-Client-ID", "demo-client")
	resourceRes := httptest.NewRecorder()
	handler.ServeHTTP(resourceRes, resourceReq)

	assert.Equal(t, http.StatusOK, resourceRes.Code)
	assert.Equal(t, "10", resourceRes.Header().Get("RateLimit-Limit"))
	assert.Equal(t, "9", resourceRes.Header().Get("RateLimit-Remaining"))

	metricsReq := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	metricsRes := httptest.NewRecorder()
	handler.ServeHTTP(metricsRes, metricsReq)

	assert.Equal(t, http.StatusOK, metricsRes.Code)

	var snapshot ratelimiter.CounterSnapshot
	err := json.Unmarshal(metricsRes.Body.Bytes(), &snapshot)
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), snapshot.Allowed)
	assert.Equal(t, uint64(0), snapshot.Denied)
	assert.Equal(t, uint64(0), snapshot.Errors)
}

func TestBuildMux_FailOpenOnLimiterErrors(t *testing.T) {
	store := &fakeStore{
		tokenBucketCheck: func(ctx context.Context, key ratelimiter.Key, rate int, burst int, window time.Duration) (ratelimiter.Result, error) {
			return ratelimiter.Result{}, errors.New("redis unavailable")
		},
	}

	handler, counters := buildMux(ratelimiter.New(store), demoConfig())

	req := httptest.NewRequest(http.MethodGet, "/api/resource", nil)
	req.Header.Set("X-Client-ID", "demo-client")
	res := httptest.NewRecorder()
	handler.ServeHTTP(res, req)

	assert.Equal(t, http.StatusOK, res.Code)

	snapshot := counters.Snapshot()
	assert.Equal(t, uint64(0), snapshot.Allowed)
	assert.Equal(t, uint64(0), snapshot.Denied)
	assert.Equal(t, uint64(1), snapshot.Errors)
}
