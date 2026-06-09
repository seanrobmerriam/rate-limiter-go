package ratelimiter

import (
	"context"
	"fmt"
	"time"
)

// PrefixedStore wraps a Store and prepends a prefix to every key.
// This is useful for multi-tenant deployments sharing a single backing store.
type PrefixedStore struct {
	prefix string
	inner  Store
}

// NewPrefixedStore creates a new PrefixedStore that prepends the given prefix
// to all keys before delegating to the inner store.
func NewPrefixedStore(prefix string, inner Store) *PrefixedStore {
	return &PrefixedStore{
		prefix: prefix,
		inner:  inner,
	}
}

func (s *PrefixedStore) prefixed(key Key) Key {
	return Key(fmt.Sprintf("%s:%s", s.prefix, key))
}

func (s *PrefixedStore) TokenBucketCheck(ctx context.Context, key Key, rate int, burst int, window time.Duration) (Result, error) {
	return s.inner.TokenBucketCheck(ctx, s.prefixed(key), rate, burst, window)
}

func (s *PrefixedStore) SlidingWindowCheck(ctx context.Context, key Key, rate int, window time.Duration) (Result, error) {
	return s.inner.SlidingWindowCheck(ctx, s.prefixed(key), rate, window)
}

func (s *PrefixedStore) Check(ctx context.Context, cfg Config) (Result, error) {
	cfg.Key = s.prefixed(cfg.Key)
	return s.inner.Check(ctx, cfg)
}

func (s *PrefixedStore) Peek(ctx context.Context, cfg Config) (State, error) {
	cfg.Key = s.prefixed(cfg.Key)
	return s.inner.Peek(ctx, cfg)
}

func (s *PrefixedStore) Reset(ctx context.Context, key Key) error {
	return s.inner.Reset(ctx, s.prefixed(key))
}

func (s *PrefixedStore) ResetMulti(ctx context.Context, keys ...Key) error {
	prefixed := make([]Key, len(keys))
	for i, k := range keys {
		prefixed[i] = s.prefixed(k)
	}
	return s.inner.ResetMulti(ctx, prefixed...)
}

func (s *PrefixedStore) Ping(ctx context.Context) error {
	return s.inner.Ping(ctx)
}

func (s *PrefixedStore) Close() error {
	return s.inner.Close()
}
