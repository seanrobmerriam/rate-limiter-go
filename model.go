package ratelimiter

import (
	"context"
	"errors"
	"time"
)

type Key string

type Algorithm string

type Status string

const (
	TokenBucket   Algorithm = "token_bucket"
	SlidingWindow Algorithm = "sliding_window"
)

const (
	Allowed Status = "allowed"
	Denied  Status = "denied"
)

type Config struct {
	Key       Key
	Algorithm Algorithm
	Rate      int
	Window    time.Duration
	BurstSize int
}

func (c Config) WithKey(key Key) Config {
	c.Key = key
	return c
}

func (c Config) Validate() error {
	if c.Algorithm == "" {
		return errors.New("algorithm is required")
	}
	if c.Algorithm != TokenBucket && c.Algorithm != SlidingWindow {
		return errors.New("invalid algorithm: must be 'token_bucket' or 'sliding_window'")
	}
	if c.Rate <= 0 {
		return errors.New("rate must be greater than 0")
	}
	if c.Window <= 0 {
		return errors.New("window must be greater than 0")
	}
	if c.BurstSize < 0 {
		return errors.New("burstSize must be non-negative")
	}
	return nil
}

type Result struct {
	Status     Status
	Remaining  int
	RetryAfter time.Duration
	ResetAt    time.Time
}

type Limiter interface {
	Check(ctx context.Context, cfg Config) (Result, error)
	Reset(ctx context.Context, key Key) error
	Close() error
}

type Store interface {
	TokenBucketCheck(ctx context.Context, key Key, rate int, burst int, window time.Duration) (Result, error)
	SlidingWindowCheck(ctx context.Context, key Key, rate int, window time.Duration) (Result, error)
	Reset(ctx context.Context, key Key) error
	Ping(ctx context.Context) error
	Close() error
}
