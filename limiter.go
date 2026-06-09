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
// Delegates to the store's Check method for algorithm dispatch.
func (l *limiter) Check(ctx context.Context, cfg Config) (Result, error) {
	if err := cfg.Validate(); err != nil {
		return Result{}, err
	}
	
	return l.store.Check(ctx, cfg)
}

// Reset clears the rate limit state for the given key.
func (l *limiter) Reset(ctx context.Context, key Key) error {
	if err := l.store.Reset(ctx, key); err != nil {
		return fmt.Errorf("ratelimiter: reset failed: %w", err)
	}
	return nil
}

// ResetMulti clears the rate limit state for the given keys.
func (l *limiter) ResetMulti(ctx context.Context, keys ...Key) error {
	if err := l.store.ResetMulti(ctx, keys...); err != nil {
		return fmt.Errorf("ratelimiter: reset multi failed: %w", err)
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
