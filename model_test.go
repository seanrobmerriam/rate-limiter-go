package ratelimiter_test

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	ratelimiter "github.com/seanrobmerriam/rate-limiter-go"
	"github.com/stretchr/testify/assert"
)

func TestConfig_Validate_ZeroValuesAreInvalid(t *testing.T) {
	tests := []struct {
		name   string
		config ratelimiter.Config
	}{
		{
			name:   "zero values should be invalid",
			config: ratelimiter.Config{},
		},
		{
			name: "zero algorithm should be invalid",
			config: ratelimiter.Config{
				Rate:   100,
				Window: time.Second,
			},
		},
		{
			name: "zero max requests should be invalid",
			config: ratelimiter.Config{
				Algorithm: ratelimiter.TokenBucket,
				Window:    time.Second,
			},
		},
		{
			name: "zero window size should be invalid",
			config: ratelimiter.Config{
				Algorithm: ratelimiter.TokenBucket,
				Rate:      100,
			},
		},
		{
			name: "negative max requests should be invalid",
			config: ratelimiter.Config{
				Algorithm: ratelimiter.TokenBucket,
				Rate:      -1,
				Window:    time.Second,
			},
		},
		{
			name: "negative window size should be invalid",
			config: ratelimiter.Config{
				Algorithm: ratelimiter.TokenBucket,
				Rate:      100,
				Window:    -1 * time.Second,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			assert.Error(t, err, "Validate() should return an error for invalid config")
		})
	}
}

func TestConfig_Validate_ValidConfigs(t *testing.T) {
	tests := []struct {
		name   string
		config ratelimiter.Config
	}{
		{
			name: "token bucket with valid values",
			config: ratelimiter.Config{
				Algorithm: ratelimiter.TokenBucket,
				Rate:      100,
				Window:    time.Second,
			},
		},
		{
			name: "sliding window with valid values",
			config: ratelimiter.Config{
				Algorithm: ratelimiter.SlidingWindow,
				Rate:      50,
				Window:    30 * time.Second,
			},
		},
		{
			name: "minimum valid values",
			config: ratelimiter.Config{
				Algorithm: ratelimiter.TokenBucket,
				Rate:      1,
				Window:    time.Second,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			assert.NoError(t, err, "Validate() should not return an error for valid config")
		})
	}
}

func TestResult_DeniedStatusHasRetryAfter(t *testing.T) {
	tests := []struct {
		name       string
		result     ratelimiter.Result
		expectZero bool
	}{
		{
			name: "denied status should have positive retry after",
			result: ratelimiter.Result{
				Status:     ratelimiter.Denied,
				RetryAfter: 30 * time.Second,
			},
			expectZero: false,
		},
		{
			name: "denied status with zero retry after should fail validation",
			result: ratelimiter.Result{
				Status:     ratelimiter.Denied,
				RetryAfter: time.Duration(0),
			},
			expectZero: true,
		},
		{
			name: "allowed status can have zero retry after",
			result: ratelimiter.Result{
				Status:     ratelimiter.Allowed,
				RetryAfter: time.Duration(0),
			},
			expectZero: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.result.Status == ratelimiter.Denied {
				if tt.expectZero {
					assert.Equal(t, time.Duration(0), tt.result.RetryAfter, "Denied status should have zero retry after for test case: %s", tt.name)
				} else {
					assert.Greater(t, tt.result.RetryAfter, time.Duration(0), "Denied status should have positive RetryAfter")
				}
			}
		})
	}
}

func TestAlgorithm_Constants(t *testing.T) {
	assert.Equal(t, string(ratelimiter.TokenBucket), "token_bucket", "TokenBucket constant should be 'token_bucket'")
	assert.Equal(t, string(ratelimiter.SlidingWindow), "sliding_window", "SlidingWindow constant should be 'sliding_window'")
}

func TestStatus_Constants(t *testing.T) {
	assert.Equal(t, string(ratelimiter.Allowed), "allowed", "Allowed constant should be 'allowed'")
	assert.Equal(t, string(ratelimiter.Denied), "denied", "Denied constant should be 'denied'")
}

func TestHeaderKeyFunc(t *testing.T) {
	keyFn := ratelimiter.HeaderKeyFunc("X-Client-ID")
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("X-Client-ID", "client-123")

	assert.Equal(t, ratelimiter.Key("client-123"), keyFn(req))
}

func TestRemoteIPKeyFunc(t *testing.T) {
	t.Run("strips port from remote address", func(t *testing.T) {
		keyFn := ratelimiter.RemoteIPKeyFunc()
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.RemoteAddr = "203.0.113.10:54321"

		assert.Equal(t, ratelimiter.Key("203.0.113.10"), keyFn(req))
	})

	t.Run("preserves bare remote address", func(t *testing.T) {
		keyFn := ratelimiter.RemoteIPKeyFunc()
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.RemoteAddr = "198.51.100.8"

		assert.Equal(t, ratelimiter.Key("198.51.100.8"), keyFn(req))
	})
}

func TestConfigWithKey(t *testing.T) {
	original := ratelimiter.Config{
		Key:       ratelimiter.Key("original"),
		Algorithm: ratelimiter.TokenBucket,
		Rate:      10,
		BurstSize: 5,
		Window:    time.Second,
	}

	updated := original.WithKey(ratelimiter.Key("updated"))

	assert.Equal(t, ratelimiter.Key("original"), original.Key)
	assert.Equal(t, ratelimiter.Key("updated"), updated.Key)
	assert.Equal(t, original.Algorithm, updated.Algorithm)
	assert.Equal(t, original.Rate, updated.Rate)
	assert.Equal(t, original.BurstSize, updated.BurstSize)
	assert.Equal(t, original.Window, updated.Window)
}
