package redis

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	ratelimiter "github.com/seanrobmerriam/rate-limiter-go"
)

const (
	tokenBucketKeyPrefix   = "rl:%s:tb"
	slidingWindowKeyPrefix = "rl:%s:sw"
	defaultTTL             = 24 * time.Hour
)

var (
	tokenBucketScript = redis.NewScript(`
		local key = KEYS[1]
		local rate = tonumber(ARGV[1])
		local burst = tonumber(ARGV[2])
		local window = tonumber(ARGV[3])
		local now = tonumber(ARGV[4])
		local ttl = tonumber(ARGV[5])
		
		local data = redis.call('HGETALL', key)
		local tokens = burst
		local lastRefill = now
		
		if #data > 0 then
			for i = 1, #data, 2 do
				if data[i] == 'tokens' then
					tokens = tonumber(data[i + 1])
				elseif data[i] == 'last_refill' then
					lastRefill = tonumber(data[i + 1])
				end
			end
		end
		
		local elapsed = now - lastRefill
		if elapsed > 0 then
			local tokensToAdd = (elapsed * rate) / window
			tokens = tokens + tokensToAdd
			if tokens > burst then
				tokens = burst
			end
		end
		
		if tokens >= 1 then
			local remaining = math.floor(tokens - 1)
			if remaining < 0 then
				remaining = 0
			end
			redis.call('HSET', key, 'tokens', tokens - 1, 'last_refill', now)
			redis.call('PEXPIRE', key, ttl)
			return {1, remaining, 0}
		else
			local retryAfter = math.ceil(((1 - tokens) * window) / rate)
			if retryAfter < 1 then
				retryAfter = 1
			end
			redis.call('HSET', key, 'tokens', tokens, 'last_refill', now)
			redis.call('PEXPIRE', key, ttl)
			return {0, 0, retryAfter}
		end
	`)

	slidingWindowScript = redis.NewScript(`
		local key = KEYS[1]
		local rate = tonumber(ARGV[1])
		local window = tonumber(ARGV[2])
		local now = tonumber(ARGV[3])
		local ttl = tonumber(ARGV[4])
		local member = ARGV[5]
		
		redis.call('ZREMRANGEBYSCORE', key, '-inf', now - window)
		
		local count = redis.call('ZCARD', key)
		
		if count < rate then
			redis.call('ZADD', key, now, member)
			redis.call('PEXPIRE', key, ttl)
			local remaining = rate - count - 1
			if remaining < 0 then
				remaining = 0
			end
			return {1, remaining, 0}
		else
			local oldest = redis.call('ZRANGE', key, 0, 0, 'WITHSCORES')
			local retryAfter = 0
			if #oldest > 0 then
				retryAfter = tonumber(oldest[2]) + window - now
				if retryAfter < 1 then
					retryAfter = 1
				end
			end
			redis.call('PEXPIRE', key, ttl)
			return {0, 0, retryAfter}
		end
	`)
)

// RedisStore implements the ratelimiter.Store interface backed by Redis.
// It uses atomic Lua scripts for token bucket and sliding window algorithms.
type RedisStore struct {
	client *redis.Client
	logger *slog.Logger
	closed atomic.Bool
}

// New creates a new Redis store with the given address.
// It returns an error if the Redis client cannot be created.
func New(addr string) (*RedisStore, error) {
	if addr == "" {
		return nil, errors.New("Redis address is required")
	}
	if _, _, err := net.SplitHostPort(addr); err != nil {
		return nil, fmt.Errorf("invalid Redis address: %w", err)
	}
	client := redis.NewClient(&redis.Options{
		Addr: addr,
	})
	return &RedisStore{
		client: client,
		logger: slog.Default(),
	}, nil
}

// NewWithClient creates a new Redis store with an existing Redis client.
// This is useful for testing or when the client needs custom configuration.
func NewWithClient(client *redis.Client) *RedisStore {
	return &RedisStore{
		client: client,
		logger: slog.Default(),
	}
}

// TokenBucketCheck checks if a request is allowed using the token bucket algorithm.
// It atomically retrieves the current token count, calculates tokens to add based on
// elapsed time since last refill, and decrements a token if available.
// Returns a Result with status Allowed/Denied, remaining tokens, retryAfter duration, and resetAt time.
func (s *RedisStore) TokenBucketCheck(ctx context.Context, key ratelimiter.Key, rate int, burst int, window time.Duration) (ratelimiter.Result, error) {
	storeKey := fmt.Sprintf(tokenBucketKeyPrefix, key)
	now := time.Now()
	ttl := int((window * 2).Milliseconds())
	if ttl <= 0 {
		ttl = int(defaultTTL.Milliseconds())
	}

	result, err := tokenBucketScript.Run(ctx, s.client, []string{storeKey},
		rate,
		burst,
		window.Milliseconds(),
		now.UnixMilli(),
		ttl,
	).Slice()

	if err != nil {
		s.logger.Error("token bucket check failed",
			slog.String("key", string(key)),
			slog.Any("error", err))
		return ratelimiter.Result{}, fmt.Errorf("token bucket check failed: %w", err)
	}

	if len(result) < 3 {
		return ratelimiter.Result{}, errors.New("unexpected result from lua script")
	}

	allowed := result[0].(int64) == 1
	remaining := int(result[1].(int64))
	retryAfter := time.Duration(result[2].(int64)) * time.Millisecond
	if !allowed && retryAfter <= 0 {
		retryAfter = time.Duration(float64(window) / float64(rate))
		if retryAfter <= 0 {
			retryAfter = time.Second
		}
	}

	if allowed {
		return ratelimiter.Result{
			Status:    ratelimiter.Allowed,
			Remaining: remaining,
			ResetAt:   now.Add(window),
		}, nil
	}
	return ratelimiter.Result{
		Status:     ratelimiter.Denied,
		Remaining:  0,
		RetryAfter: retryAfter,
		ResetAt:    now.Add(window),
	}, nil
}

// SlidingWindowCheck checks if a request is allowed using the sliding window algorithm.
// It atomically removes expired entries from the sorted set, counts remaining requests,
// and adds a new entry if under the rate limit.
// Returns a Result with status Allowed/Denied, remaining slots, retryAfter duration, and resetAt time.
func (s *RedisStore) SlidingWindowCheck(ctx context.Context, key ratelimiter.Key, rate int, window time.Duration) (ratelimiter.Result, error) {
	storeKey := fmt.Sprintf(slidingWindowKeyPrefix, key)
	now := time.Now()
	ttl := int((window * 2).Milliseconds())
	if ttl <= 0 {
		ttl = int(defaultTTL.Milliseconds())
	}

	member := uuid.New().String()

	result, err := slidingWindowScript.Run(ctx, s.client, []string{storeKey},
		rate,
		window.Milliseconds(),
		now.UnixMilli(),
		ttl,
		member,
	).Slice()

	if err != nil {
		s.logger.Error("sliding window check failed",
			slog.String("key", string(key)),
			slog.Any("error", err))
		return ratelimiter.Result{}, fmt.Errorf("sliding window check failed: %w", err)
	}

	if len(result) < 3 {
		return ratelimiter.Result{}, errors.New("unexpected result from lua script")
	}

	allowed := result[0].(int64) == 1
	remaining := int(result[1].(int64))
	retryAfter := time.Duration(result[2].(int64)) * time.Millisecond

	if allowed {
		return ratelimiter.Result{
			Status:    ratelimiter.Allowed,
			Remaining: remaining,
			ResetAt:   now.Add(window),
		}, nil
	}

	if retryAfter <= 0 {
		retryAfter = time.Duration(float64(window) / float64(rate))
		if retryAfter <= 0 {
			retryAfter = time.Second
		}
	}

	return ratelimiter.Result{
		Status:     ratelimiter.Denied,
		Remaining:  0,
		RetryAfter: retryAfter,
		ResetAt:    now.Add(window),
	}, nil
}

// Reset removes all rate limit state (both token bucket and sliding window) for the given key.
func (s *RedisStore) Reset(ctx context.Context, key ratelimiter.Key) error {
	tbKey := fmt.Sprintf(tokenBucketKeyPrefix, key)
	swKey := fmt.Sprintf(slidingWindowKeyPrefix, key)

	_, err := s.client.Del(ctx, tbKey, swKey).Result()
	if err != nil {
		s.logger.Error("reset failed",
			slog.String("key", string(key)),
			slog.Any("error", err))
		return fmt.Errorf("reset failed: %w", err)
	}

	return nil
}

// Ping returns nil if the Redis connection is healthy, otherwise returns an error.
func (s *RedisStore) Ping(ctx context.Context) error {
	_, err := s.client.Ping(ctx).Result()
	if err != nil {
		s.logger.Error("ping failed", slog.Any("error", err))
		return fmt.Errorf("ping failed: %w", err)
	}
	return nil
}

// Close closes the Redis client connection.
func (s *RedisStore) Close() error {
	if s.closed.Swap(true) {
		return nil
	}
	return s.client.Close()
}
