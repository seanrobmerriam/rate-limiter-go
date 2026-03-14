package ratelimiter_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ratelimiter/ratelimiter"
	"github.com/stretchr/testify/assert"
)

type mockStore struct {
	tokenBucketCheckFunc   func(ctx context.Context, key ratelimiter.Key, rate int, burst int, window time.Duration) (ratelimiter.Result, error)
	slidingWindowCheckFunc func(ctx context.Context, key ratelimiter.Key, rate int, window time.Duration) (ratelimiter.Result, error)
	resetFunc              func(ctx context.Context, key ratelimiter.Key) error
	pingFunc               func(ctx context.Context) error
	closeFunc              func() error

	tokenBucketCheckCalled   bool
	slidingWindowCheckCalled bool
	resetCalled              bool
	pingCalled               bool
	closeCalled              bool
}

func (m *mockStore) TokenBucketCheck(ctx context.Context, key ratelimiter.Key, rate int, burst int, window time.Duration) (ratelimiter.Result, error) {
	m.tokenBucketCheckCalled = true
	if m.tokenBucketCheckFunc != nil {
		return m.tokenBucketCheckFunc(ctx, key, rate, burst, window)
	}
	return ratelimiter.Result{}, nil
}

func (m *mockStore) SlidingWindowCheck(ctx context.Context, key ratelimiter.Key, rate int, window time.Duration) (ratelimiter.Result, error) {
	m.slidingWindowCheckCalled = true
	if m.slidingWindowCheckFunc != nil {
		return m.slidingWindowCheckFunc(ctx, key, rate, window)
	}
	return ratelimiter.Result{}, nil
}

func (m *mockStore) Reset(ctx context.Context, key ratelimiter.Key) error {
	m.resetCalled = true
	if m.resetFunc != nil {
		return m.resetFunc(ctx, key)
	}
	return nil
}

func (m *mockStore) Ping(ctx context.Context) error {
	m.pingCalled = true
	if m.pingFunc != nil {
		return m.pingFunc(ctx)
	}
	return nil
}

func (m *mockStore) Close() error {
	m.closeCalled = true
	if m.closeFunc != nil {
		return m.closeFunc()
	}
	return nil
}

func TestLimiter_Check_WithInvalidConfig_ReturnsError_NeverCallsStore(t *testing.T) {
	ctx := context.Background()
	store := &mockStore{}
	limiter := ratelimiter.New(store)

	tests := []struct {
		name   string
		config ratelimiter.Config
	}{
		{
			name:   "empty config returns error",
			config: ratelimiter.Config{},
		},
		{
			name: "missing algorithm returns error",
			config: ratelimiter.Config{
				Key:       "test-key",
				Rate:      100,
				Window:    time.Second,
				BurstSize: 10,
			},
		},
		{
			name: "invalid algorithm returns error",
			config: ratelimiter.Config{
				Key:       "test-key",
				Algorithm: "invalid",
				Rate:      100,
				Window:    time.Second,
				BurstSize: 10,
			},
		},
		{
			name: "zero rate returns error",
			config: ratelimiter.Config{
				Key:       "test-key",
				Algorithm: ratelimiter.TokenBucket,
				Rate:      0,
				Window:    time.Second,
				BurstSize: 10,
			},
		},
		{
			name: "negative rate returns error",
			config: ratelimiter.Config{
				Key:       "test-key",
				Algorithm: ratelimiter.TokenBucket,
				Rate:      -1,
				Window:    time.Second,
				BurstSize: 10,
			},
		},
		{
			name: "zero window returns error",
			config: ratelimiter.Config{
				Key:       "test-key",
				Algorithm: ratelimiter.TokenBucket,
				Rate:      100,
				Window:    0,
				BurstSize: 10,
			},
		},
		{
			name: "negative window returns error",
			config: ratelimiter.Config{
				Key:       "test-key",
				Algorithm: ratelimiter.TokenBucket,
				Rate:      100,
				Window:    -time.Second,
				BurstSize: 10,
			},
		},
		{
			name: "negative burst size returns error",
			config: ratelimiter.Config{
				Key:       "test-key",
				Algorithm: ratelimiter.TokenBucket,
				Rate:      100,
				Window:    time.Second,
				BurstSize: -1,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store.tokenBucketCheckCalled = false
			store.slidingWindowCheckCalled = false

			result, err := limiter.Check(ctx, tt.config)

			assert.Error(t, err, "Check should return error for invalid config")
			assert.Zero(t, result.Status, "Result status should be zero for invalid config")
			assert.False(t, store.tokenBucketCheckCalled, "TokenBucketCheck should not be called")
			assert.False(t, store.slidingWindowCheckCalled, "SlidingWindowCheck should not be called")
		})
	}
}

func TestLimiter_Check_DispatchesToCorrectStoreMethod(t *testing.T) {
	ctx := context.Background()

	t.Run("TokenBucket algorithm calls TokenBucketCheck", func(t *testing.T) {
		store := &mockStore{
			tokenBucketCheckFunc: func(ctx context.Context, key ratelimiter.Key, rate int, burst int, window time.Duration) (ratelimiter.Result, error) {
				assert.Equal(t, ratelimiter.Key("test-key"), key)
				assert.Equal(t, 100, rate)
				assert.Equal(t, 50, burst)
				assert.Equal(t, time.Second, window)
				return ratelimiter.Result{Status: ratelimiter.Allowed, Remaining: 49}, nil
			},
		}
		limiter := ratelimiter.New(store)

		config := ratelimiter.Config{
			Key:       "test-key",
			Algorithm: ratelimiter.TokenBucket,
			Rate:      100,
			BurstSize: 50,
			Window:    time.Second,
		}

		result, err := limiter.Check(ctx, config)

		assert.NoError(t, err)
		assert.True(t, store.tokenBucketCheckCalled, "TokenBucketCheck should be called")
		assert.False(t, store.slidingWindowCheckCalled, "SlidingWindowCheck should not be called")
		assert.Equal(t, ratelimiter.Allowed, result.Status)
		assert.Equal(t, 49, result.Remaining)
	})

	t.Run("SlidingWindow algorithm calls SlidingWindowCheck", func(t *testing.T) {
		store := &mockStore{
			slidingWindowCheckFunc: func(ctx context.Context, key ratelimiter.Key, rate int, window time.Duration) (ratelimiter.Result, error) {
				assert.Equal(t, ratelimiter.Key("test-key"), key)
				assert.Equal(t, 50, rate)
				assert.Equal(t, 30*time.Second, window)
				return ratelimiter.Result{Status: ratelimiter.Allowed, Remaining: 49}, nil
			},
		}
		limiter := ratelimiter.New(store)

		config := ratelimiter.Config{
			Key:       "test-key",
			Algorithm: ratelimiter.SlidingWindow,
			Rate:      50,
			Window:    30 * time.Second,
		}

		result, err := limiter.Check(ctx, config)

		assert.NoError(t, err)
		assert.False(t, store.tokenBucketCheckCalled, "TokenBucketCheck should not be called")
		assert.True(t, store.slidingWindowCheckCalled, "SlidingWindowCheck should be called")
		assert.Equal(t, ratelimiter.Allowed, result.Status)
		assert.Equal(t, 49, result.Remaining)
	})
}

func TestLimiter_Check_WithStoreError_Propagates(t *testing.T) {
	ctx := context.Background()

	t.Run("TokenBucket store error propagates directly", func(t *testing.T) {
		storeErr := errors.New("redis connection failed")
		store := &mockStore{
			tokenBucketCheckFunc: func(ctx context.Context, key ratelimiter.Key, rate int, burst int, window time.Duration) (ratelimiter.Result, error) {
				return ratelimiter.Result{}, storeErr
			},
		}
		limiter := ratelimiter.New(store)

		config := ratelimiter.Config{
			Key:       "test-key",
			Algorithm: ratelimiter.TokenBucket,
			Rate:      100,
			BurstSize: 50,
			Window:    time.Second,
		}

		result, err := limiter.Check(ctx, config)

		assert.Error(t, err, "Error should be returned when store returns error")
		assert.Equal(t, storeErr, err, "Store error should propagate directly")
		assert.Zero(t, result.Status, "Result should be zero when error occurs")
		assert.True(t, store.tokenBucketCheckCalled, "TokenBucketCheck should be called")
	})

	t.Run("SlidingWindow store error propagates directly", func(t *testing.T) {
		storeErr := errors.New("redis connection failed")
		store := &mockStore{
			slidingWindowCheckFunc: func(ctx context.Context, key ratelimiter.Key, rate int, window time.Duration) (ratelimiter.Result, error) {
				return ratelimiter.Result{}, storeErr
			},
		}
		limiter := ratelimiter.New(store)

		config := ratelimiter.Config{
			Key:       "test-key",
			Algorithm: ratelimiter.SlidingWindow,
			Rate:      50,
			Window:    30 * time.Second,
		}

		result, err := limiter.Check(ctx, config)

		assert.Error(t, err, "Error should be returned when store returns error")
		assert.Equal(t, storeErr, err, "Store error should propagate directly")
		assert.Zero(t, result.Status, "Result should be zero when error occurs")
		assert.True(t, store.slidingWindowCheckCalled, "SlidingWindowCheck should be called")
	})

	t.Run("unsupported algorithm returns validation error", func(t *testing.T) {
		store := &mockStore{}
		limiter := ratelimiter.New(store)

		config := ratelimiter.Config{
			Key:       "test-key",
			Algorithm: "unknown_algorithm",
			Rate:      100,
			BurstSize: 50,
			Window:    time.Second,
		}

		result, err := limiter.Check(ctx, config)

		assert.Error(t, err, "Error should be returned for unsupported algorithm")
		assert.Contains(t, err.Error(), "invalid algorithm", "Error should mention invalid algorithm")
		assert.Zero(t, result.Status, "Result should be zero for unsupported algorithm")
		assert.False(t, store.tokenBucketCheckCalled, "No store method should be called")
		assert.False(t, store.slidingWindowCheckCalled, "No store method should be called")
	})
}

func TestLimiter_Reset_DelegatesToStore(t *testing.T) {
	ctx := context.Background()

	t.Run("Reset delegates to store correctly", func(t *testing.T) {
		var capturedKey ratelimiter.Key
		store := &mockStore{
			resetFunc: func(ctx context.Context, key ratelimiter.Key) error {
				capturedKey = key
				return nil
			},
		}
		limiter := ratelimiter.New(store)

		key := ratelimiter.Key("test-key")
		err := limiter.Reset(ctx, key)

		assert.NoError(t, err, "Reset should not return error when store succeeds")
		assert.True(t, store.resetCalled, "Reset should be called on store")
		assert.Equal(t, key, capturedKey, "Key should be passed to store")
	})

	t.Run("Reset wraps store error", func(t *testing.T) {
		storeErr := errors.New("redis reset failed")
		store := &mockStore{
			resetFunc: func(ctx context.Context, key ratelimiter.Key) error {
				return storeErr
			},
		}
		limiter := ratelimiter.New(store)

		key := ratelimiter.Key("test-key")
		err := limiter.Reset(ctx, key)

		assert.Error(t, err, "Error should be returned when store returns error")
		assert.Contains(t, err.Error(), "ratelimiter: reset failed", "Error should be wrapped")
		assert.Contains(t, err.Error(), "redis reset failed", "Original error should be preserved")
		assert.True(t, store.resetCalled, "Reset should be called on store")
	})
}

func TestLimiter_Close_DelegatesToStore(t *testing.T) {
	t.Run("Close delegates to store correctly", func(t *testing.T) {
		store := &mockStore{}
		limiter := ratelimiter.New(store)

		err := limiter.Close()

		assert.NoError(t, err, "Close should not return error when store succeeds")
		assert.True(t, store.closeCalled, "Close should be called on store")
	})

	t.Run("Close wraps store error", func(t *testing.T) {
		storeErr := errors.New("redis close failed")
		store := &mockStore{
			closeFunc: func() error {
				return storeErr
			},
		}
		limiter := ratelimiter.New(store)

		err := limiter.Close()

		assert.Error(t, err, "Error should be returned when store returns error")
		assert.Contains(t, err.Error(), "ratelimiter: close failed", "Error should be wrapped")
		assert.Contains(t, err.Error(), "redis close failed", "Original error should be preserved")
		assert.True(t, store.closeCalled, "Close should be called on store")
	})
}

func TestLimiter_Check_PassesCorrectParametersToStore(t *testing.T) {
	ctx := context.Background()

	t.Run("TokenBucket passes all parameters", func(t *testing.T) {
		var capturedRate, capturedBurst int
		var capturedWindow time.Duration
		store := &mockStore{
			tokenBucketCheckFunc: func(ctx context.Context, key ratelimiter.Key, rate int, burst int, window time.Duration) (ratelimiter.Result, error) {
				capturedRate = rate
				capturedBurst = burst
				capturedWindow = window
				return ratelimiter.Result{Status: ratelimiter.Allowed}, nil
			},
		}
		limiter := ratelimiter.New(store)

		config := ratelimiter.Config{
			Key:       "my-key",
			Algorithm: ratelimiter.TokenBucket,
			Rate:      200,
			BurstSize: 100,
			Window:    5 * time.Second,
		}

		_, err := limiter.Check(ctx, config)

		assert.NoError(t, err)
		assert.Equal(t, 200, capturedRate)
		assert.Equal(t, 100, capturedBurst)
		assert.Equal(t, 5*time.Second, capturedWindow)
	})

	t.Run("SlidingWindow passes all parameters", func(t *testing.T) {
		var capturedRate int
		var capturedWindow time.Duration
		store := &mockStore{
			slidingWindowCheckFunc: func(ctx context.Context, key ratelimiter.Key, rate int, window time.Duration) (ratelimiter.Result, error) {
				capturedRate = rate
				capturedWindow = window
				return ratelimiter.Result{Status: ratelimiter.Allowed}, nil
			},
		}
		limiter := ratelimiter.New(store)

		config := ratelimiter.Config{
			Key:       "my-key",
			Algorithm: ratelimiter.SlidingWindow,
			Rate:      150,
			Window:    10 * time.Second,
		}

		_, err := limiter.Check(ctx, config)

		assert.NoError(t, err)
		assert.Equal(t, 150, capturedRate)
		assert.Equal(t, 10*time.Second, capturedWindow)
	})
}

func TestLimiter_Check_ContextIsPassedToStore(t *testing.T) {
	t.Run("TokenBucket receives context", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var receivedCtx context.Context
		store := &mockStore{
			tokenBucketCheckFunc: func(ctx context.Context, key ratelimiter.Key, rate int, burst int, window time.Duration) (ratelimiter.Result, error) {
				receivedCtx = ctx
				return ratelimiter.Result{Status: ratelimiter.Allowed}, nil
			},
		}
		limiter := ratelimiter.New(store)

		config := ratelimiter.Config{
			Key:       "test-key",
			Algorithm: ratelimiter.TokenBucket,
			Rate:      100,
			BurstSize: 50,
			Window:    time.Second,
		}

		_, err := limiter.Check(ctx, config)

		assert.NoError(t, err)
		assert.Equal(t, ctx, receivedCtx, "Context should be passed to store")
	})

	t.Run("SlidingWindow receives context", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var receivedCtx context.Context
		store := &mockStore{
			slidingWindowCheckFunc: func(ctx context.Context, key ratelimiter.Key, rate int, window time.Duration) (ratelimiter.Result, error) {
				receivedCtx = ctx
				return ratelimiter.Result{Status: ratelimiter.Allowed}, nil
			},
		}
		limiter := ratelimiter.New(store)

		config := ratelimiter.Config{
			Key:       "test-key",
			Algorithm: ratelimiter.SlidingWindow,
			Rate:      50,
			Window:    time.Second,
		}

		_, err := limiter.Check(ctx, config)

		assert.NoError(t, err)
		assert.Equal(t, ctx, receivedCtx, "Context should be passed to store")
	})
}

func TestLimiter_Check_KeyIsPassedToStore(t *testing.T) {
	ctx := context.Background()

	t.Run("TokenBucket receives key", func(t *testing.T) {
		var capturedKey ratelimiter.Key
		store := &mockStore{
			tokenBucketCheckFunc: func(ctx context.Context, key ratelimiter.Key, rate int, burst int, window time.Duration) (ratelimiter.Result, error) {
				capturedKey = key
				return ratelimiter.Result{Status: ratelimiter.Allowed}, nil
			},
		}
		limiter := ratelimiter.New(store)

		config := ratelimiter.Config{
			Key:       "unique-test-key-123",
			Algorithm: ratelimiter.TokenBucket,
			Rate:      100,
			BurstSize: 50,
			Window:    time.Second,
		}

		_, err := limiter.Check(ctx, config)

		assert.NoError(t, err)
		assert.Equal(t, ratelimiter.Key("unique-test-key-123"), capturedKey)
	})

	t.Run("SlidingWindow receives key", func(t *testing.T) {
		var capturedKey ratelimiter.Key
		store := &mockStore{
			slidingWindowCheckFunc: func(ctx context.Context, key ratelimiter.Key, rate int, window time.Duration) (ratelimiter.Result, error) {
				capturedKey = key
				return ratelimiter.Result{Status: ratelimiter.Allowed}, nil
			},
		}
		limiter := ratelimiter.New(store)

		config := ratelimiter.Config{
			Key:       "unique-test-key-456",
			Algorithm: ratelimiter.SlidingWindow,
			Rate:      50,
			Window:    time.Second,
		}

		_, err := limiter.Check(ctx, config)

		assert.NoError(t, err)
		assert.Equal(t, ratelimiter.Key("unique-test-key-456"), capturedKey)
	})
}
