package ratelimiter

import (
	"context"
	"log/slog"
	"sync/atomic"
	"time"
)

type EventKind string

const (
	EventAllowed EventKind = "allowed"
	EventDenied  EventKind = "denied"
	EventError   EventKind = "error"
)

type Event struct {
	Kind       EventKind
	Key        Key
	Algorithm  Algorithm
	Status     Status
	Remaining  int
	RetryAfter time.Duration
	ResetAt    time.Time
	Err        error
	Duration   time.Duration
}

type Observer interface {
	Observe(ctx context.Context, event Event)
}

type ObserverFunc func(ctx context.Context, event Event)

func (f ObserverFunc) Observe(ctx context.Context, event Event) {
	if f != nil {
		f(ctx, event)
	}
}

type CounterSnapshot struct {
	Allowed uint64
	Denied  uint64
	Errors  uint64
}

type Counters struct {
	allowed atomic.Uint64
	denied  atomic.Uint64
	errors  atomic.Uint64
}

func NewCounters() *Counters {
	return &Counters{}
}

func (c *Counters) Observe(ctx context.Context, event Event) {
	switch event.Kind {
	case EventAllowed:
		c.allowed.Add(1)
	case EventDenied:
		c.denied.Add(1)
	case EventError:
		c.errors.Add(1)
	}
}

func (c *Counters) Snapshot() CounterSnapshot {
	return CounterSnapshot{
		Allowed: c.allowed.Load(),
		Denied:  c.denied.Load(),
		Errors:  c.errors.Load(),
	}
}

type SlogObserver struct {
	logger *slog.Logger
}

func NewSlogObserver(logger *slog.Logger) *SlogObserver {
	if logger == nil {
		logger = slog.Default()
	}
	return &SlogObserver{logger: logger}
}

func (o *SlogObserver) Observe(ctx context.Context, event Event) {
	if o == nil || o.logger == nil {
		return
	}

	attrs := []any{
		"kind", event.Kind,
		"key", event.Key,
		"algorithm", event.Algorithm,
		"status", event.Status,
		"remaining", event.Remaining,
		"retry_after", event.RetryAfter,
		"reset_at", event.ResetAt,
		"duration", event.Duration,
	}
	if event.Err != nil {
		attrs = append(attrs, "error", event.Err)
	}

	o.logger.InfoContext(ctx, "rate limiter event", attrs...)
}