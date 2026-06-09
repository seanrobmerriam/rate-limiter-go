package ratelimiter

import (
	"crypto/sha256"
	"fmt"
	"net/http"
	"strings"
)

// KeyNormalizer transforms a raw key string before it is used by the store.
type KeyNormalizer func(string) string

// NormalizeKeyFunc wraps a key-extraction function with a normalizer.
// The normalizer is applied to the key returned by the wrapped function.
func NormalizeKeyFunc(keyFn func(*http.Request) Key, normalizer KeyNormalizer) func(*http.Request) Key {
	return func(r *http.Request) Key {
		key := keyFn(r)
		if normalizer != nil {
			return Key(normalizer(string(key)))
		}
		return key
	}
}

// KeyFuncNoopNormalizer returns the key unchanged.
func KeyFuncNoopNormalizer() KeyNormalizer {
	return func(s string) string { return s }
}

// KeyFuncLowercaseNormalizer converts the key to lowercase.
func KeyFuncLowercaseNormalizer() KeyNormalizer {
	return strings.ToLower
}

// KeyFuncHashNormalizer returns a SHA-256 hex hash of the key.
// This is useful for fixed-width keys or avoiding key information leakage.
func KeyFuncHashNormalizer() KeyNormalizer {
	return func(s string) string {
		h := sha256.Sum256([]byte(s))
		return fmt.Sprintf("%x", h)
	}
}
