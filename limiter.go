package ratelimiter

import (
	"context"
	"fmt"
)

type limiter struct {
	store Store
}

// New creates a new rate limiter with the given store.
func New(store Store) Limiter {
	return &limiter{
		store: store,
	}
}

// Check determines if the request for the given key is allowed based on the configured algorithm.
// It validates the config first and returns an error if invalid.
// Dispatches to TokenBucketCheck or SlidingWindowCheck based on cfg.Algorithm.
func (l *limiter) Check(ctx context.Context, cfg Config) (Result, error) {
	if err := cfg.Validate(); err != nil {
		return Result{}, err
	}

	switch cfg.Algorithm {
	case TokenBucket:
		return l.store.TokenBucketCheck(ctx, cfg.Key, cfg.Rate, cfg.BurstSize, cfg.Window)
	case SlidingWindow:
		return l.store.SlidingWindowCheck(ctx, cfg.Key, cfg.Rate, cfg.Window)
	default:
		return Result{}, fmt.Errorf("ratelimiter: check failed: %w", fmt.Errorf("unsupported algorithm: %s", cfg.Algorithm))
	}
}

// Reset clears the rate limit state for the given key.
func (l *limiter) Reset(ctx context.Context, key Key) error {
	if err := l.store.Reset(ctx, key); err != nil {
		return fmt.Errorf("ratelimiter: reset failed: %w", err)
	}
	return nil
}

// Close closes the rate limiter and its underlying store.
func (l *limiter) Close() error {
	if err := l.store.Close(); err != nil {
		return fmt.Errorf("ratelimiter: close failed: %w", err)
	}
	return nil
}
