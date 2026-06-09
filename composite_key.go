package ratelimiter

import (
	"net/http"
	"strings"
)

// CompositeKeyFunc combines multiple key-extraction functions into one.
// The resulting key is the parts joined by the separator.
// Empty parts are skipped.
func CompositeKeyFunc(fns ...func(*http.Request) Key) func(*http.Request) Key {
	return func(r *http.Request) Key {
		parts := make([]string, 0, len(fns))
		for _, fn := range fns {
			if k := fn(r); k != "" {
				parts = append(parts, string(k))
			}
		}
		if len(parts) == 0 {
			return ""
		}
		return Key(strings.Join(parts, ":"))
	}
}
