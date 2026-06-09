package ratelimiter

import (
	"encoding/json"
	"net/http"
)

// MetricsHandler returns an http.HandlerFunc that writes the Counters snapshot as JSON.
// This is a reusable component for exposing rate limit metrics on a /metrics endpoint.
func MetricsHandler(counters *Counters) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(counters.Snapshot())
	}
}
