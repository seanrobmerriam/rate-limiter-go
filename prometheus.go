package ratelimiter

import (
	"context"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	registerOnce sync.Once
	reqTotal     *prometheus.CounterVec
	reqDuration  *prometheus.HistogramVec
)

func initMetrics() {
	registerOnce.Do(func() {
		reqTotal = promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: "rate_limiter",
			Subsystem: "requests",
			Name:      "total",
			Help:      "Total rate limiter requests by result kind.",
		}, []string{"kind", "algorithm"})

		reqDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "rate_limiter",
			Subsystem: "requests",
			Name:      "duration_seconds",
			Help:      "Latency of rate limiter checks in seconds.",
			Buckets:   prometheus.DefBuckets,
		}, []string{"kind", "algorithm"})
	})
}

type PrometheusObserver struct{}

func NewPrometheusObserver() *PrometheusObserver {
	initMetrics()
	return &PrometheusObserver{}
}

func (o *PrometheusObserver) Observe(ctx context.Context, event Event) {
	kind := string(event.Kind)
	algo := string(event.Algorithm)

	reqTotal.WithLabelValues(kind, algo).Inc()
	reqDuration.WithLabelValues(kind, algo).Observe(event.Duration.Seconds())
}
