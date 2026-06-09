package memory

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	ratelimiter "github.com/seanrobmerriam/rate-limiter-go"
)

type MemoryStore struct {
	mu             sync.RWMutex
	tokenBuckets   map[ratelimiter.Key]*tokenBucketState
	slidingWindows map[ratelimiter.Key]*slidingWindowState
	leakyBuckets   map[ratelimiter.Key]*leakyBucketState
	fixedWindows   map[ratelimiter.Key]*fixedWindowState
	closed         bool
	expiryTicker   *time.Ticker
	expiryStop     chan struct{}
}

type tokenBucketState struct {
	tokens     float64
	lastRefill time.Time
	zeroTime   time.Time
	rate       int
	burst      int
	window     time.Duration
}

type slidingWindowState struct {
	requests []time.Time
	rate     int
	window   time.Duration
}

type leakyBucketState struct {
	queue    []time.Time
	rate     int
	burst    int
	window   time.Duration
}

type fixedWindowState struct {
	windowStart time.Time
	count       int
	rate        int
	window      time.Duration
}

// New creates a new in-memory rate limiter store.
// It starts a background goroutine for cleaning up expired entries.
// The caller should call Close when done to stop the background goroutine.
func New() *MemoryStore {
	store := &MemoryStore{
		tokenBuckets:   make(map[ratelimiter.Key]*tokenBucketState),
		slidingWindows: make(map[ratelimiter.Key]*slidingWindowState),
		leakyBuckets:   make(map[ratelimiter.Key]*leakyBucketState),
		fixedWindows:   make(map[ratelimiter.Key]*fixedWindowState),
		expiryStop:     make(chan struct{}),
	}
	store.expiryTicker = time.NewTicker(time.Minute)
	go store.runExpiry()
	return store
}

func (s *MemoryStore) runExpiry() {
	for {
		select {
		case <-s.expiryTicker.C:
			s.expireOldEntries()
		case <-s.expiryStop:
			return
		}
	}
}

func (s *MemoryStore) expireOldEntries() {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()

	for key, tb := range s.tokenBuckets {
		expiryTime := tb.lastRefill
		if !tb.zeroTime.IsZero() {
			expiryTime = tb.zeroTime
		}
		expiryThreshold := expiryTime.Add(2 * tb.window)
		if now.After(expiryThreshold) {
			delete(s.tokenBuckets, key)
		}
	}

	for key, sw := range s.slidingWindows {
		if len(sw.requests) == 0 {
			continue
		}
		oldestRequest := sw.requests[0]
		expiryThreshold := oldestRequest.Add(2 * sw.window)
		if now.After(expiryThreshold) {
			delete(s.slidingWindows, key)
		}
	}
}

func (s *MemoryStore) checkContext(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}

// Check dispatches to the correct algorithm based on cfg.Algorithm.
func (s *MemoryStore) Check(ctx context.Context, cfg ratelimiter.Config) (ratelimiter.Result, error) {
	switch cfg.Algorithm {
	case ratelimiter.TokenBucket:
		return s.TokenBucketCheck(ctx, cfg.Key, cfg.Rate, cfg.BurstSize, cfg.Window)
	case ratelimiter.SlidingWindow:
		return s.SlidingWindowCheck(ctx, cfg.Key, cfg.Rate, cfg.Window)
	case ratelimiter.LeakyBucket:
		return s.LeakyBucketCheck(ctx, cfg.Key, cfg.Rate, cfg.BurstSize, cfg.Window)
	case ratelimiter.FixedWindow:
		return s.FixedWindowCheck(ctx, cfg.Key, cfg.Rate, cfg.Window)
	default:
		return ratelimiter.Result{}, fmt.Errorf("memory: unsupported algorithm: %s", cfg.Algorithm)
	}
}

// TokenBucketCheck checks if a request is allowed using the token bucket algorithm.
// It calculates tokens to add based on time elapsed since last refill or zero time,
// caps at burst, and decrements tokens if available.
func (s *MemoryStore) TokenBucketCheck(ctx context.Context, key ratelimiter.Key, rate int, burst int, window time.Duration) (ratelimiter.Result, error) {
	if err := s.checkContext(ctx); err != nil {
		return ratelimiter.Result{}, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.checkContext(ctx); err != nil {
		return ratelimiter.Result{}, err
	}

	if s.closed {
		return ratelimiter.Result{}, errors.New("store is closed")
	}

	now := time.Now()
	tb, exists := s.tokenBuckets[key]

	if !exists {
		if burst <= 0 {
			retryAfter := time.Duration(float64(window) / float64(rate))
			if retryAfter <= 0 {
				retryAfter = time.Second
			}
			return ratelimiter.Result{
				Status:     ratelimiter.Denied,
				Remaining:  0,
				RetryAfter: retryAfter,
				ResetAt:    now.Add(window),
			}, nil
		}
		tb = &tokenBucketState{
			tokens:     float64(burst) - 1,
			lastRefill: now,
			zeroTime:   time.Time{},
			rate:       rate,
			burst:      burst,
			window:     window,
		}
		s.tokenBuckets[key] = tb
		return ratelimiter.Result{
			Status:    ratelimiter.Allowed,
			Remaining: burst - 1,
			ResetAt:   now.Add(window),
		}, nil
	}

	if tb.rate != rate || tb.burst != burst || tb.window != window {
		oldBurst := tb.burst
		tb.rate = rate
		tb.burst = burst
		tb.window = window

		if oldBurst > 0 && burst > 0 {
			tb.tokens = tb.tokens * float64(burst) / float64(oldBurst)
			if tb.tokens > float64(burst) {
				tb.tokens = float64(burst)
			}
		} else {
			tb.tokens = float64(burst)
		}

		tb.lastRefill = now
		tb.zeroTime = time.Time{}

		if tb.tokens >= 1 {
			tb.tokens--
			remaining := int(tb.tokens)
			if remaining < 0 {
				remaining = 0
			}
			return ratelimiter.Result{
				Status:    ratelimiter.Allowed,
				Remaining: remaining,
				ResetAt:   now.Add(window),
			}, nil
		}

		if tb.tokens < 1 && tb.zeroTime.IsZero() {
			tb.zeroTime = now
		}

		needed := 1.0 - tb.tokens
		retryAfter := time.Duration((needed * float64(window)) / float64(rate))
		if retryAfter <= 0 {
			retryAfter = time.Duration(float64(window) / float64(rate))
		}
		if retryAfter <= 0 {
			retryAfter = time.Second
		}

		return ratelimiter.Result{
			Status:     ratelimiter.Denied,
			Remaining:  0,
			RetryAfter: retryAfter,
			ResetAt:    now.Add(window),
		}, nil
	}

	refillFrom := tb.lastRefill
	if !tb.zeroTime.IsZero() {
		refillFrom = tb.zeroTime
	}

	elapsed := now.Sub(refillFrom)
	var newTokens float64
	if elapsed >= tb.window {
		newTokens = float64(tb.burst)
	} else {
		tokensToAdd := (float64(elapsed) * float64(tb.rate)) / float64(tb.window)
		newTokens = tb.tokens + tokensToAdd
		if newTokens > float64(tb.burst) {
			newTokens = float64(tb.burst)
		}
	}

	if newTokens >= 1 {
		tb.tokens = newTokens
		tb.lastRefill = now
		tb.zeroTime = time.Time{}
		tb.tokens--
		remaining := int(tb.tokens)
		if remaining < 0 {
			remaining = 0
		}
		return ratelimiter.Result{
			Status:    ratelimiter.Allowed,
			Remaining: remaining,
			ResetAt:   now.Add(window),
		}, nil
	}

	tb.tokens = newTokens
	if newTokens < 1 && tb.zeroTime.IsZero() {
		tb.zeroTime = now
	}

	needed := 1.0 - tb.tokens
	retryAfter := time.Duration((needed * float64(tb.window)) / float64(tb.rate))
	if retryAfter <= 0 {
		retryAfter = time.Duration(float64(tb.window) / float64(tb.rate))
	}
	if retryAfter <= 0 {
		retryAfter = time.Second
	}

	return ratelimiter.Result{
		Status:     ratelimiter.Denied,
		Remaining:  0,
		RetryAfter: retryAfter,
		ResetAt:    now.Add(window),
	}, nil
}

// SlidingWindowCheck checks if a request is allowed using the sliding window algorithm.
// It removes expired timestamps and allows if the number of requests in the window is less than rate.
func (s *MemoryStore) SlidingWindowCheck(ctx context.Context, key ratelimiter.Key, rate int, window time.Duration) (ratelimiter.Result, error) {
	if err := s.checkContext(ctx); err != nil {
		return ratelimiter.Result{}, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.checkContext(ctx); err != nil {
		return ratelimiter.Result{}, err
	}

	if s.closed {
		return ratelimiter.Result{}, errors.New("store is closed")
	}

	now := time.Now()
	sw, exists := s.slidingWindows[key]

	if !exists {
		sw = &slidingWindowState{
			requests: make([]time.Time, 0),
			rate:     rate,
			window:   window,
		}
		s.slidingWindows[key] = sw
		sw.requests = append(sw.requests, now)
		return ratelimiter.Result{
			Status:    ratelimiter.Allowed,
			Remaining: rate - 1,
			ResetAt:   now.Add(window),
		}, nil
	}

	validRequests := make([]time.Time, 0, len(sw.requests))
	for _, reqTime := range sw.requests {
		if now.Sub(reqTime) < window {
			validRequests = append(validRequests, reqTime)
		}
	}
	sw.requests = validRequests

	if len(sw.requests) < rate {
		sw.requests = append(sw.requests, now)
		remaining := rate - len(sw.requests)
		return ratelimiter.Result{
			Status:    ratelimiter.Allowed,
			Remaining: remaining,
			ResetAt:   now.Add(window),
		}, nil
	}

	oldestRequest := sw.requests[0]
	retryAfter := window - now.Sub(oldestRequest)
	if retryAfter <= 0 {
		retryAfter = time.Second
	}

	return ratelimiter.Result{
		Status:     ratelimiter.Denied,
		Remaining:  0,
		RetryAfter: retryAfter,
		ResetAt:    now.Add(window),
	}, nil
}

// LeakyBucketCheck checks if a request is allowed using the leaky bucket algorithm.
// Requests are added to a queue that drains at a fixed rate (rate per window).
// If the queue is full (burst), the request is denied.
func (s *MemoryStore) LeakyBucketCheck(ctx context.Context, key ratelimiter.Key, rate int, burst int, window time.Duration) (ratelimiter.Result, error) {
	if err := s.checkContext(ctx); err != nil {
		return ratelimiter.Result{}, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.checkContext(ctx); err != nil {
		return ratelimiter.Result{}, err
	}

	if s.closed {
		return ratelimiter.Result{}, errors.New("store is closed")
	}

	now := time.Now()
	lb, exists := s.leakyBuckets[key]

	if !exists {
		if burst <= 0 {
			retryAfter := time.Duration(float64(window) / float64(rate))
			if retryAfter <= 0 {
				retryAfter = time.Second
			}
			return ratelimiter.Result{
				Status:     ratelimiter.Denied,
				Remaining:  0,
				RetryAfter: retryAfter,
				ResetAt:    now.Add(window),
			}, nil
		}
		lb = &leakyBucketState{
			queue:  []time.Time{now},
			rate:   rate,
			burst:  burst,
			window: window,
		}
		s.leakyBuckets[key] = lb
		return ratelimiter.Result{
			Status:    ratelimiter.Allowed,
			Remaining: burst - 1,
			ResetAt:   now.Add(window),
		}, nil
	}

	drainDuration := float64(window) / float64(lb.rate)
	drainCutoff := now.Add(-time.Duration(int64(len(lb.queue)) * int64(drainDuration)))

	valid := make([]time.Time, 0, len(lb.queue))
	for _, t := range lb.queue {
		if t.After(drainCutoff) {
			valid = append(valid, t)
		}
	}
	lb.queue = valid

	if len(lb.queue) < lb.burst {
		lb.queue = append(lb.queue, now)
		remaining := lb.burst - len(lb.queue)
		return ratelimiter.Result{
			Status:    ratelimiter.Allowed,
			Remaining: remaining,
			ResetAt:   now.Add(window),
		}, nil
	}

	oldest := lb.queue[0]
	retryAfter := time.Duration(float64(window)/float64(lb.rate)) - now.Sub(oldest)
	if retryAfter <= 0 {
		retryAfter = time.Duration(float64(window) / float64(lb.rate))
	}
	if retryAfter <= 0 {
		retryAfter = time.Second
	}

	return ratelimiter.Result{
		Status:     ratelimiter.Denied,
		Remaining:  0,
		RetryAfter: retryAfter,
		ResetAt:    now.Add(window),
	}, nil
}

// FixedWindowCheck checks if a request is allowed using the fixed window algorithm.
// A counter resets at fixed window boundaries. If the counter is below rate, allow.
func (s *MemoryStore) FixedWindowCheck(ctx context.Context, key ratelimiter.Key, rate int, window time.Duration) (ratelimiter.Result, error) {
	if err := s.checkContext(ctx); err != nil {
		return ratelimiter.Result{}, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.checkContext(ctx); err != nil {
		return ratelimiter.Result{}, err
	}

	if s.closed {
		return ratelimiter.Result{}, errors.New("store is closed")
	}

	now := time.Now()
	fw, exists := s.fixedWindows[key]

	if !exists || now.Sub(fw.windowStart) >= fw.window {
		fw = &fixedWindowState{
			windowStart: now,
			count:       1,
			rate:        rate,
			window:      window,
		}
		s.fixedWindows[key] = fw
		return ratelimiter.Result{
			Status:    ratelimiter.Allowed,
			Remaining: rate - 1,
			ResetAt:   now.Add(window),
		}, nil
	}

	if fw.count < fw.rate {
		fw.count++
		remaining := fw.rate - fw.count
		return ratelimiter.Result{
			Status:    ratelimiter.Allowed,
			Remaining: remaining,
			ResetAt:   fw.windowStart.Add(fw.window),
		}, nil
	}

	retryAfter := fw.windowStart.Add(fw.window).Sub(now)
	if retryAfter <= 0 {
		retryAfter = time.Duration(float64(window) / float64(rate))
	}
	if retryAfter <= 0 {
		retryAfter = time.Second
	}

	return ratelimiter.Result{
		Status:     ratelimiter.Denied,
		Remaining:  0,
		RetryAfter: retryAfter,
		ResetAt:    fw.windowStart.Add(fw.window),
	}, nil
}

// Reset removes all rate limit state for the given key.
func (s *MemoryStore) Reset(ctx context.Context, key ratelimiter.Key) error {
	if err := s.checkContext(ctx); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.checkContext(ctx); err != nil {
		return err
	}

	if s.closed {
		return errors.New("store is closed")
	}

	delete(s.tokenBuckets, key)
	delete(s.slidingWindows, key)
	delete(s.leakyBuckets, key)
	delete(s.fixedWindows, key)

	return nil
}

// ResetMulti removes all rate limit state for the given keys.
func (s *MemoryStore) ResetMulti(ctx context.Context, keys ...ratelimiter.Key) error {
	if err := s.checkContext(ctx); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.checkContext(ctx); err != nil {
		return err
	}

	if s.closed {
		return errors.New("store is closed")
	}

	for _, key := range keys {
		delete(s.tokenBuckets, key)
		delete(s.slidingWindows, key)
		delete(s.leakyBuckets, key)
		delete(s.fixedWindows, key)
	}

	return nil
}

// Ping returns nil if the store is operational.
func (s *MemoryStore) Ping(ctx context.Context) error {
	if err := s.checkContext(ctx); err != nil {
		return err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	if err := s.checkContext(ctx); err != nil {
		return err
	}

	if s.closed {
		return errors.New("store is closed")
	}

	return nil
}

// Close stops the background goroutine and releases resources.
// After Close is called, all methods return an error.
func (s *MemoryStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil
	}

	s.closed = true
	s.expiryTicker.Stop()
	close(s.expiryStop)

	return nil
}
