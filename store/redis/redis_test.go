package redis

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ratelimiter/ratelimiter"
	redigo "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/testcontainers/testcontainers-go/modules/redis"
)

func isLocalRedisAvailable() bool {
	conn, err := net.DialTimeout("tcp", "localhost:6379", 2*time.Second)
	if err != nil {
		return false
	}
	conn.Close()
	return true
}

func tryLocalRedis() *redigo.Client {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	client := redigo.NewClient(&redigo.Options{
		Addr: "localhost:6379",
	})
	err := client.Ping(ctx).Err()
	if err != nil {
		client.Close()
		return nil
	}
	return client
}

func startRedisContainer(t *testing.T) (*redis.RedisContainer, string) {
	ctx := context.Background()

	if isLocalRedisAvailable() {
		client := tryLocalRedis()
		if client != nil {
			t.Log("Using local Redis at localhost:6379")
			return nil, "localhost:6379"
		}
	}

	redisContainer, err := redis.Run(ctx, "redis:7-alpine")
	if err != nil {
		t.Skipf("Failed to start Redis container: %v", err)
	}

	connStr, err := redisContainer.ConnectionString(ctx)
	if err != nil {
		t.Skipf("Failed to get Redis connection string: %v", err)
	}

	return redisContainer, connStr
}

func TestTokenBucket_BurstAllowsFirstRequests(t *testing.T) {
	ctx := context.Background()
	container, connStr := startRedisContainer(t)
	if container != nil {
		defer container.Terminate(ctx)
	}

	store, err := New(connStr)
	if err != nil {
		t.Fatalf("Failed to create Redis store: %v", err)
	}
	defer store.Close()

	store.client.FlushDB(ctx)

	key := ratelimiter.Key("test-burst")
	cfg := ratelimiter.Config{
		Key:       key,
		Algorithm: ratelimiter.TokenBucket,
		Rate:      5,
		Window:    time.Second,
		BurstSize: 10,
	}

	for i := 0; i < 10; i++ {
		result, err := store.TokenBucketCheck(ctx, key, cfg.Rate, cfg.BurstSize, cfg.Window)
		assert.NoError(t, err)
		assert.Equal(t, ratelimiter.Allowed, result.Status)
	}
}

func TestTokenBucket_11thRequestDenied(t *testing.T) {
	ctx := context.Background()
	container, connStr := startRedisContainer(t)
	if container != nil {
		defer container.Terminate(ctx)
	}

	store, err := New(connStr)
	if err != nil {
		t.Fatalf("Failed to create Redis store: %v", err)
	}
	defer store.Close()

	store.client.FlushDB(ctx)

	key := ratelimiter.Key("test-11th")
	cfg := ratelimiter.Config{
		Key:       key,
		Algorithm: ratelimiter.TokenBucket,
		Rate:      5,
		Window:    time.Second,
		BurstSize: 10,
	}

	for i := 0; i < 10; i++ {
		result, err := store.TokenBucketCheck(ctx, key, cfg.Rate, cfg.BurstSize, cfg.Window)
		assert.NoError(t, err)
		assert.Equal(t, ratelimiter.Allowed, result.Status)
	}

	result, err := store.TokenBucketCheck(ctx, key, cfg.Rate, cfg.BurstSize, cfg.Window)
	assert.NoError(t, err)
	assert.Equal(t, ratelimiter.Denied, result.Status)
	assert.Greater(t, result.RetryAfter, time.Duration(0))
}

func TestTokenBucket_RefillsAfterWindow(t *testing.T) {
	ctx := context.Background()
	container, connStr := startRedisContainer(t)
	if container != nil {
		defer container.Terminate(ctx)
	}

	store, err := New(connStr)
	if err != nil {
		t.Fatalf("Failed to create Redis store: %v", err)
	}
	defer store.Close()

	store.client.FlushDB(ctx)

	key := ratelimiter.Key("test-refill")
	cfg := ratelimiter.Config{
		Key:       key,
		Algorithm: ratelimiter.TokenBucket,
		Rate:      5,
		Window:    time.Second,
		BurstSize: 10,
	}

	for i := 0; i < 10; i++ {
		result, err := store.TokenBucketCheck(ctx, key, cfg.Rate, cfg.BurstSize, cfg.Window)
		assert.NoError(t, err)
		assert.Equal(t, ratelimiter.Allowed, result.Status)
	}

	result, err := store.TokenBucketCheck(ctx, key, cfg.Rate, cfg.BurstSize, cfg.Window)
	assert.NoError(t, err)
	assert.Equal(t, ratelimiter.Denied, result.Status)

	time.Sleep(1100 * time.Millisecond)

	result, err = store.TokenBucketCheck(ctx, key, cfg.Rate, cfg.BurstSize, cfg.Window)
	assert.NoError(t, err)
	assert.Equal(t, ratelimiter.Allowed, result.Status)
}

func TestSlidingWindow_FirstThreeAllowed(t *testing.T) {
	ctx := context.Background()
	container, connStr := startRedisContainer(t)
	if container != nil {
		defer container.Terminate(ctx)
	}

	store, err := New(connStr)
	if err != nil {
		t.Fatalf("Failed to create Redis store: %v", err)
	}
	defer store.Close()

	// Clear any leftover state
	store.client.FlushDB(ctx)

	key := ratelimiter.Key("test-sliding-first")
	cfg := ratelimiter.Config{
		Key:       key,
		Algorithm: ratelimiter.SlidingWindow,
		Rate:      3,
		Window:    time.Second,
	}

	for i := 0; i < 3; i++ {
		result, err := store.SlidingWindowCheck(ctx, key, cfg.Rate, cfg.Window)
		assert.NoError(t, err)
		assert.Equal(t, ratelimiter.Allowed, result.Status)
	}
}

func TestSlidingWindow_4thDenied(t *testing.T) {
	ctx := context.Background()
	container, connStr := startRedisContainer(t)
	if container != nil {
		defer container.Terminate(ctx)
	}

	store, err := New(connStr)
	if err != nil {
		t.Fatalf("Failed to create Redis store: %v", err)
	}
	defer store.Close()

	// Clear any leftover state
	store.client.FlushDB(ctx)

	key := ratelimiter.Key("test-sliding-4th")
	cfg := ratelimiter.Config{
		Key:       key,
		Algorithm: ratelimiter.SlidingWindow,
		Rate:      3,
		Window:    time.Second,
	}

	for i := 0; i < 3; i++ {
		result, err := store.SlidingWindowCheck(ctx, key, cfg.Rate, cfg.Window)
		assert.NoError(t, err)
		assert.Equal(t, ratelimiter.Allowed, result.Status)
	}

	result, err := store.SlidingWindowCheck(ctx, key, cfg.Rate, cfg.Window)
	assert.NoError(t, err)
	assert.Equal(t, ratelimiter.Denied, result.Status)
	assert.Greater(t, result.RetryAfter, time.Duration(0))
}

func TestSlidingWindow_WindowSlides(t *testing.T) {
	ctx := context.Background()
	container, connStr := startRedisContainer(t)
	if container != nil {
		defer container.Terminate(ctx)
	}

	store, err := New(connStr)
	if err != nil {
		t.Fatalf("Failed to create Redis store: %v", err)
	}
	defer store.Close()

	// Clear any leftover state
	store.client.FlushDB(ctx)

	key := ratelimiter.Key("test-sliding-slide")
	cfg := ratelimiter.Config{
		Key:       key,
		Algorithm: ratelimiter.SlidingWindow,
		Rate:      3,
		Window:    time.Second,
	}

	for i := 0; i < 3; i++ {
		result, err := store.SlidingWindowCheck(ctx, key, cfg.Rate, cfg.Window)
		assert.NoError(t, err)
		assert.Equal(t, ratelimiter.Allowed, result.Status)
	}

	result, err := store.SlidingWindowCheck(ctx, key, cfg.Rate, cfg.Window)
	assert.NoError(t, err)
	assert.Equal(t, ratelimiter.Denied, result.Status)

	time.Sleep(1100 * time.Millisecond)

	result, err = store.SlidingWindowCheck(ctx, key, cfg.Rate, cfg.Window)
	assert.NoError(t, err)
	assert.Equal(t, ratelimiter.Allowed, result.Status)
}

func TestSlidingWindow_MultipleWindows(t *testing.T) {
	ctx := context.Background()
	container, connStr := startRedisContainer(t)
	if container != nil {
		defer container.Terminate(ctx)
	}

	store, err := New(connStr)
	if err != nil {
		t.Fatalf("Failed to create Redis store: %v", err)
	}
	defer store.Close()

	// Clear any leftover state
	store.client.FlushDB(ctx)

	key := ratelimiter.Key("test-sliding-multiple")
	cfg := ratelimiter.Config{
		Key:       key,
		Algorithm: ratelimiter.SlidingWindow,
		Rate:      2,
		Window:    500 * time.Millisecond,
	}

	result, err := store.SlidingWindowCheck(ctx, key, cfg.Rate, cfg.Window)
	assert.NoError(t, err)
	assert.Equal(t, ratelimiter.Allowed, result.Status)

	result, err = store.SlidingWindowCheck(ctx, key, cfg.Rate, cfg.Window)
	assert.NoError(t, err)
	assert.Equal(t, ratelimiter.Allowed, result.Status)

	result, err = store.SlidingWindowCheck(ctx, key, cfg.Rate, cfg.Window)
	assert.NoError(t, err)
	assert.Equal(t, ratelimiter.Denied, result.Status)

	time.Sleep(600 * time.Millisecond)

	result, err = store.SlidingWindowCheck(ctx, key, cfg.Rate, cfg.Window)
	assert.NoError(t, err)
	assert.Equal(t, ratelimiter.Allowed, result.Status)

	time.Sleep(600 * time.Millisecond)

	result, err = store.SlidingWindowCheck(ctx, key, cfg.Rate, cfg.Window)
	assert.NoError(t, err)
	assert.Equal(t, ratelimiter.Allowed, result.Status)

	result, err = store.SlidingWindowCheck(ctx, key, cfg.Rate, cfg.Window)
	assert.NoError(t, err)
	assert.Equal(t, ratelimiter.Allowed, result.Status)

	result, err = store.SlidingWindowCheck(ctx, key, cfg.Rate, cfg.Window)
	assert.NoError(t, err)
	assert.Equal(t, ratelimiter.Denied, result.Status)
}

func TestTokenBucket_DifferentKeys(t *testing.T) {
	ctx := context.Background()
	container, connStr := startRedisContainer(t)
	if container != nil {
		defer container.Terminate(ctx)
	}

	store, err := New(connStr)
	if err != nil {
		t.Fatalf("Failed to create Redis store: %v", err)
	}
	defer store.Close()

	// Clear any leftover state
	store.client.FlushDB(ctx)

	key1 := ratelimiter.Key("key1")
	key2 := ratelimiter.Key("key2")
	cfg := ratelimiter.Config{
		Key:       key1,
		Algorithm: ratelimiter.TokenBucket,
		Rate:      5,
		Window:    time.Second,
		BurstSize: 10,
	}

	for i := 0; i < 10; i++ {
		result, err := store.TokenBucketCheck(ctx, key1, cfg.Rate, cfg.BurstSize, cfg.Window)
		assert.NoError(t, err)
		assert.Equal(t, ratelimiter.Allowed, result.Status)
	}

	for i := 0; i < 10; i++ {
		result, err := store.TokenBucketCheck(ctx, key2, cfg.Rate, cfg.BurstSize, cfg.Window)
		assert.NoError(t, err)
		assert.Equal(t, ratelimiter.Allowed, result.Status)
	}

	result, err := store.TokenBucketCheck(ctx, key1, cfg.Rate, cfg.BurstSize, cfg.Window)
	assert.NoError(t, err)
	assert.Equal(t, ratelimiter.Denied, result.Status)

	result, err = store.TokenBucketCheck(ctx, key2, cfg.Rate, cfg.BurstSize, cfg.Window)
	assert.NoError(t, err)
	assert.Equal(t, ratelimiter.Denied, result.Status)
}

func TestSlidingWindow_DifferentKeys(t *testing.T) {
	ctx := context.Background()
	container, connStr := startRedisContainer(t)
	if container != nil {
		defer container.Terminate(ctx)
	}

	store, err := New(connStr)
	if err != nil {
		t.Fatalf("Failed to create Redis store: %v", err)
	}
	defer store.Close()

	// Clear any leftover state
	store.client.FlushDB(ctx)

	key1 := ratelimiter.Key("sw-key1")
	key2 := ratelimiter.Key("sw-key2")
	cfg := ratelimiter.Config{
		Key:       key1,
		Algorithm: ratelimiter.SlidingWindow,
		Rate:      3,
		Window:    time.Second,
	}

	for i := 0; i < 3; i++ {
		result, err := store.SlidingWindowCheck(ctx, key1, cfg.Rate, cfg.Window)
		assert.NoError(t, err)
		assert.Equal(t, ratelimiter.Allowed, result.Status)
	}

	for i := 0; i < 3; i++ {
		result, err := store.SlidingWindowCheck(ctx, key2, cfg.Rate, cfg.Window)
		assert.NoError(t, err)
		assert.Equal(t, ratelimiter.Allowed, result.Status)
	}

	result, err := store.SlidingWindowCheck(ctx, key1, cfg.Rate, cfg.Window)
	assert.NoError(t, err)
	assert.Equal(t, ratelimiter.Denied, result.Status)

	result, err = store.SlidingWindowCheck(ctx, key2, cfg.Rate, cfg.Window)
	assert.NoError(t, err)
	assert.Equal(t, ratelimiter.Denied, result.Status)
}

func TestReset_ClearsTokenBucketState(t *testing.T) {
	ctx := context.Background()
	container, connStr := startRedisContainer(t)
	if container != nil {
		defer container.Terminate(ctx)
	}

	store, err := New(connStr)
	if err != nil {
		t.Fatalf("Failed to create Redis store: %v", err)
	}
	defer store.Close()

	// Clear any leftover state
	store.client.FlushDB(ctx)

	key := ratelimiter.Key("test-reset-tb")
	cfg := ratelimiter.Config{
		Key:       key,
		Algorithm: ratelimiter.TokenBucket,
		Rate:      5,
		Window:    time.Second,
		BurstSize: 10,
	}

	for i := 0; i < 10; i++ {
		result, err := store.TokenBucketCheck(ctx, key, cfg.Rate, cfg.BurstSize, cfg.Window)
		assert.NoError(t, err)
		assert.Equal(t, ratelimiter.Allowed, result.Status)
	}

	result, err := store.TokenBucketCheck(ctx, key, cfg.Rate, cfg.BurstSize, cfg.Window)
	assert.NoError(t, err)
	assert.Equal(t, ratelimiter.Denied, result.Status)

	err = store.Reset(ctx, key)
	assert.NoError(t, err)

	result, err = store.TokenBucketCheck(ctx, key, cfg.Rate, cfg.BurstSize, cfg.Window)
	assert.NoError(t, err)
	assert.Equal(t, ratelimiter.Allowed, result.Status)
	assert.Equal(t, 9, result.Remaining)
}

func TestReset_ClearsSlidingWindowState(t *testing.T) {
	ctx := context.Background()
	container, connStr := startRedisContainer(t)
	if container != nil {
		defer container.Terminate(ctx)
	}

	store, err := New(connStr)
	if err != nil {
		t.Fatalf("Failed to create Redis store: %v", err)
	}
	defer store.Close()

	// Clear any leftover state
	store.client.FlushDB(ctx)

	key := ratelimiter.Key("test-reset-sw")
	cfg := ratelimiter.Config{
		Key:       key,
		Algorithm: ratelimiter.SlidingWindow,
		Rate:      3,
		Window:    time.Second,
	}

	for i := 0; i < 3; i++ {
		result, err := store.SlidingWindowCheck(ctx, key, cfg.Rate, cfg.Window)
		assert.NoError(t, err)
		assert.Equal(t, ratelimiter.Allowed, result.Status)
	}

	result, err := store.SlidingWindowCheck(ctx, key, cfg.Rate, cfg.Window)
	assert.NoError(t, err)
	assert.Equal(t, ratelimiter.Denied, result.Status)

	err = store.Reset(ctx, key)
	assert.NoError(t, err)

	result, err = store.SlidingWindowCheck(ctx, key, cfg.Rate, cfg.Window)
	assert.NoError(t, err)
	assert.Equal(t, ratelimiter.Allowed, result.Status)
	assert.Equal(t, 2, result.Remaining)
}

func TestReset_NonExistentKey(t *testing.T) {
	ctx := context.Background()
	container, connStr := startRedisContainer(t)
	if container != nil {
		defer container.Terminate(ctx)
	}

	store, err := New(connStr)
	if err != nil {
		t.Fatalf("Failed to create Redis store: %v", err)
	}
	defer store.Close()

	key := ratelimiter.Key("nonexistent")

	err = store.Reset(ctx, key)
	assert.NoError(t, err)

	result, err := store.TokenBucketCheck(ctx, key, 5, 10, time.Second)
	assert.NoError(t, err)
	assert.Equal(t, ratelimiter.Allowed, result.Status)
}

func TestPing_ReturnsNil(t *testing.T) {
	ctx := context.Background()
	container, connStr := startRedisContainer(t)
	if container != nil {
		defer container.Terminate(ctx)
	}

	store, err := New(connStr)
	if err != nil {
		t.Fatalf("Failed to create Redis store: %v", err)
	}
	defer store.Close()

	err = store.Ping(ctx)
	assert.NoError(t, err)
}

func TestPing_AfterOperations(t *testing.T) {
	ctx := context.Background()
	container, connStr := startRedisContainer(t)
	if container != nil {
		defer container.Terminate(ctx)
	}

	store, err := New(connStr)
	if err != nil {
		t.Fatalf("Failed to create Redis store: %v", err)
	}
	defer store.Close()

	key := ratelimiter.Key("test-ping")

	for i := 0; i < 100; i++ {
		store.TokenBucketCheck(ctx, key, 5, 10, time.Second)
	}

	err = store.Ping(ctx)
	assert.NoError(t, err)
}

func TestClose_StopsCleanly(t *testing.T) {
	ctx := context.Background()
	container, connStr := startRedisContainer(t)
	if container != nil {
		defer container.Terminate(ctx)
	}

	store, err := New(connStr)
	if err != nil {
		t.Fatalf("Failed to create Redis store: %v", err)
	}

	err = store.Close()
	assert.NoError(t, err)

	_, err = store.TokenBucketCheck(ctx, "key", 5, 10, time.Second)
	assert.Error(t, err)
}

func TestClose_CanBeCalledMultipleTimes(t *testing.T) {
	ctx := context.Background()
	container, connStr := startRedisContainer(t)
	if container != nil {
		defer container.Terminate(ctx)
	}

	store, err := New(connStr)
	if err != nil {
		t.Fatalf("Failed to create Redis store: %v", err)
	}

	err = store.Close()
	assert.NoError(t, err)

	err = store.Close()
	assert.NoError(t, err)
}

func TestConcurrentAccess_TokenBucket(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping race test in short mode")
	}

	ctx := context.Background()
	container, connStr := startRedisContainer(t)
	if container != nil {
		defer container.Terminate(ctx)
	}

	store, err := New(connStr)
	if err != nil {
		t.Fatalf("Failed to create Redis store: %v", err)
	}
	defer store.Close()

	key := ratelimiter.Key("test-concurrent-tb")
	var wg sync.WaitGroup
	numGoroutines := 50
	requestsPerGoroutine := 20

	var deniedCount int32
	var allowedCount int32

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < requestsPerGoroutine; j++ {
				result, err := store.TokenBucketCheck(ctx, key, 5, 10, time.Second)
				if err == nil {
					if result.Status == ratelimiter.Allowed {
						atomic.AddInt32(&allowedCount, 1)
					} else {
						atomic.AddInt32(&deniedCount, 1)
					}
				}
			}
		}()
	}

	wg.Wait()

	total := atomic.LoadInt32(&allowedCount) + atomic.LoadInt32(&deniedCount)
	assert.Equal(t, int32(numGoroutines*requestsPerGoroutine), total)
}

func TestConcurrentAccess_SlidingWindow(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping race test in short mode")
	}

	ctx := context.Background()
	container, connStr := startRedisContainer(t)
	if container != nil {
		defer container.Terminate(ctx)
	}

	store, err := New(connStr)
	if err != nil {
		t.Fatalf("Failed to create Redis store: %v", err)
	}
	defer store.Close()

	key := ratelimiter.Key("test-concurrent-sw")
	var wg sync.WaitGroup
	numGoroutines := 50
	requestsPerGoroutine := 20

	var deniedCount int32
	var allowedCount int32

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < requestsPerGoroutine; j++ {
				result, err := store.SlidingWindowCheck(ctx, key, 3, time.Second)
				if err == nil {
					if result.Status == ratelimiter.Allowed {
						atomic.AddInt32(&allowedCount, 1)
					} else {
						atomic.AddInt32(&deniedCount, 1)
					}
				}
			}
		}()
	}

	wg.Wait()

	total := atomic.LoadInt32(&allowedCount) + atomic.LoadInt32(&deniedCount)
	assert.Equal(t, int32(numGoroutines*requestsPerGoroutine), total)
}

func TestConcurrentAccess_MultipleKeys(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping race test in short mode")
	}

	ctx := context.Background()
	container, connStr := startRedisContainer(t)
	if container != nil {
		defer container.Terminate(ctx)
	}

	store, err := New(connStr)
	if err != nil {
		t.Fatalf("Failed to create Redis store: %v", err)
	}
	defer store.Close()

	var wg sync.WaitGroup
	numKeys := 10
	numGoroutines := 10

	for k := 0; k < numKeys; k++ {
		key := ratelimiter.Key(string(rune('a' + k)))
		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := 0; j < 50; j++ {
					store.TokenBucketCheck(ctx, key, 5, 10, time.Second)
				}
			}()
		}
	}

	wg.Wait()
}

func TestTokenBucket_ZeroBurstSize(t *testing.T) {
	ctx := context.Background()
	container, connStr := startRedisContainer(t)
	if container != nil {
		defer container.Terminate(ctx)
	}

	store, err := New(connStr)
	if err != nil {
		t.Fatalf("Failed to create Redis store: %v", err)
	}
	defer store.Close()

	key := ratelimiter.Key("test-zero-burst")

	result, err := store.TokenBucketCheck(ctx, key, 5, 0, time.Second)
	assert.NoError(t, err)
	assert.Equal(t, ratelimiter.Denied, result.Status)

	result, err = store.TokenBucketCheck(ctx, key, 5, 0, time.Second)
	assert.NoError(t, err)
	assert.Equal(t, ratelimiter.Denied, result.Status)
}

func TestTokenBucket_RefillRate(t *testing.T) {
	ctx := context.Background()
	container, connStr := startRedisContainer(t)
	if container != nil {
		defer container.Terminate(ctx)
	}

	store, err := New(connStr)
	if err != nil {
		t.Fatalf("Failed to create Redis store: %v", err)
	}
	defer store.Close()

	key := ratelimiter.Key("test-refill-rate")
	cfg := ratelimiter.Config{
		Key:       key,
		Algorithm: ratelimiter.TokenBucket,
		Rate:      2,
		Window:    time.Second,
		BurstSize: 2,
	}

	result, err := store.TokenBucketCheck(ctx, key, cfg.Rate, cfg.BurstSize, cfg.Window)
	assert.NoError(t, err)
	assert.Equal(t, ratelimiter.Allowed, result.Status)
	assert.Equal(t, 1, result.Remaining)

	result, err = store.TokenBucketCheck(ctx, key, cfg.Rate, cfg.BurstSize, cfg.Window)
	assert.NoError(t, err)
	assert.Equal(t, ratelimiter.Allowed, result.Status)
	assert.Equal(t, 0, result.Remaining)

	result, err = store.TokenBucketCheck(ctx, key, cfg.Rate, cfg.BurstSize, cfg.Window)
	assert.NoError(t, err)
	assert.Equal(t, ratelimiter.Denied, result.Status)

	time.Sleep(600 * time.Millisecond)

	result, err = store.TokenBucketCheck(ctx, key, cfg.Rate, cfg.BurstSize, cfg.Window)
	assert.NoError(t, err)
	assert.Equal(t, ratelimiter.Allowed, result.Status)
	assert.Equal(t, 0, result.Remaining)

	result, err = store.TokenBucketCheck(ctx, key, cfg.Rate, cfg.BurstSize, cfg.Window)
	assert.NoError(t, err)
	assert.Equal(t, ratelimiter.Denied, result.Status)

	time.Sleep(600 * time.Millisecond)

	result, err = store.TokenBucketCheck(ctx, key, cfg.Rate, cfg.BurstSize, cfg.Window)
	assert.NoError(t, err)
	assert.Equal(t, ratelimiter.Allowed, result.Status)
	assert.Equal(t, 0, result.Remaining)
}

func TestSlidingWindow_Rate1(t *testing.T) {
	ctx := context.Background()
	container, connStr := startRedisContainer(t)
	if container != nil {
		defer container.Terminate(ctx)
	}

	store, err := New(connStr)
	if err != nil {
		t.Fatalf("Failed to create Redis store: %v", err)
	}
	defer store.Close()

	key := ratelimiter.Key("test-sw-rate1")

	result, err := store.SlidingWindowCheck(ctx, key, 1, time.Second)
	assert.NoError(t, err)
	assert.Equal(t, ratelimiter.Allowed, result.Status)
	assert.Equal(t, 0, result.Remaining)

	result, err = store.SlidingWindowCheck(ctx, key, 1, time.Second)
	assert.NoError(t, err)
	assert.Equal(t, ratelimiter.Denied, result.Status)

	time.Sleep(1100 * time.Millisecond)

	result, err = store.SlidingWindowCheck(ctx, key, 1, time.Second)
	assert.NoError(t, err)
	assert.Equal(t, ratelimiter.Allowed, result.Status)
	assert.Equal(t, 0, result.Remaining)
}

func TestTokenBucket_ResultFields(t *testing.T) {
	ctx := context.Background()
	container, connStr := startRedisContainer(t)
	if container != nil {
		defer container.Terminate(ctx)
	}

	store, err := New(connStr)
	if err != nil {
		t.Fatalf("Failed to create Redis store: %v", err)
	}
	defer store.Close()

	key := ratelimiter.Key("test-result-fields")

	result, err := store.TokenBucketCheck(ctx, key, 5, 10, time.Second)
	assert.NoError(t, err)
	assert.Equal(t, ratelimiter.Allowed, result.Status)
	assert.Equal(t, 9, result.Remaining)
	assert.Equal(t, time.Duration(0), result.RetryAfter)
	assert.True(t, result.ResetAt.After(time.Now()))

	result, err = store.TokenBucketCheck(ctx, key, 5, 10, time.Second)
	assert.NoError(t, err)
	assert.Equal(t, ratelimiter.Allowed, result.Status)
	assert.Equal(t, 8, result.Remaining)
}

func TestSlidingWindow_ResultFields(t *testing.T) {
	ctx := context.Background()
	container, connStr := startRedisContainer(t)
	if container != nil {
		defer container.Terminate(ctx)
	}

	store, err := New(connStr)
	if err != nil {
		t.Fatalf("Failed to create Redis store: %v", err)
	}
	defer store.Close()

	key := ratelimiter.Key("test-sw-result-fields")

	result, err := store.SlidingWindowCheck(ctx, key, 3, time.Second)
	assert.NoError(t, err)
	assert.Equal(t, ratelimiter.Allowed, result.Status)
	assert.Equal(t, 2, result.Remaining)
	assert.Equal(t, time.Duration(0), result.RetryAfter)
	assert.True(t, result.ResetAt.After(time.Now()) || result.ResetAt.Equal(time.Now()))
}

func TestReset_AfterClose(t *testing.T) {
	ctx := context.Background()
	container, connStr := startRedisContainer(t)
	if container != nil {
		defer container.Terminate(ctx)
	}

	store, err := New(connStr)
	if err != nil {
		t.Fatalf("Failed to create Redis store: %v", err)
	}
	store.Close()

	err = store.Reset(ctx, "key")
	assert.Error(t, err)
}

func TestTokenBucket_AfterClose(t *testing.T) {
	ctx := context.Background()
	container, connStr := startRedisContainer(t)
	if container != nil {
		defer container.Terminate(ctx)
	}

	store, err := New(connStr)
	if err != nil {
		t.Fatalf("Failed to create Redis store: %v", err)
	}
	store.Close()

	_, err = store.TokenBucketCheck(ctx, "key", 5, 10, time.Second)
	assert.Error(t, err)
}

func TestSlidingWindow_AfterClose(t *testing.T) {
	ctx := context.Background()
	container, connStr := startRedisContainer(t)
	if container != nil {
		defer container.Terminate(ctx)
	}

	store, err := New(connStr)
	if err != nil {
		t.Fatalf("Failed to create Redis store: %v", err)
	}
	store.Close()

	_, err = store.SlidingWindowCheck(ctx, "key", 3, time.Second)
	assert.Error(t, err)
}

func TestPing_AfterClose(t *testing.T) {
	ctx := context.Background()
	container, connStr := startRedisContainer(t)
	if container != nil {
		defer container.Terminate(ctx)
	}

	store, err := New(connStr)
	if err != nil {
		t.Fatalf("Failed to create Redis store: %v", err)
	}
	store.Close()

	err = store.Ping(ctx)
	assert.Error(t, err)
}

func TestSlidingWindow_RequestsAtWindowBoundary(t *testing.T) {
	ctx := context.Background()
	container, connStr := startRedisContainer(t)
	if container != nil {
		defer container.Terminate(ctx)
	}

	store, err := New(connStr)
	if err != nil {
		t.Fatalf("Failed to create Redis store: %v", err)
	}
	defer store.Close()

	// Clear any leftover state
	store.client.FlushDB(ctx)

	key := ratelimiter.Key("test-sliding-boundary")
	cfg := ratelimiter.Config{
		Key:       key,
		Algorithm: ratelimiter.SlidingWindow,
		Rate:      3,
		Window:    time.Second,
	}

	for i := 0; i < 3; i++ {
		result, err := store.SlidingWindowCheck(ctx, key, cfg.Rate, cfg.Window)
		assert.NoError(t, err)
		assert.Equal(t, ratelimiter.Allowed, result.Status)
	}

	time.Sleep(500 * time.Millisecond)

	result, err := store.SlidingWindowCheck(ctx, key, cfg.Rate, cfg.Window)
	assert.NoError(t, err)
	assert.Equal(t, ratelimiter.Denied, result.Status)

	time.Sleep(600 * time.Millisecond)

	result, err = store.SlidingWindowCheck(ctx, key, cfg.Rate, cfg.Window)
	assert.NoError(t, err)
	assert.Equal(t, ratelimiter.Allowed, result.Status)
}

func TestNew_InvalidConnectionString(t *testing.T) {
	_, err := New("invalid-connection-string")
	assert.Error(t, err)
}

func TestNew_EmptyConnectionString(t *testing.T) {
	_, err := New("")
	assert.Error(t, err)
}
