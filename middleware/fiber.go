package middleware

import (
	"net/http"
	"strconv"
	"time"

	"github.com/gofiber/fiber/v2"
	ratelimiter "github.com/seanrobmerriam/rate-limiter-go"
)

// Fiber returns a Fiber middleware handler that applies rate limiting.
// keyFn extracts the rate limit key from the Fiber context.
func Fiber(limiter ratelimiter.Limiter, cfg ratelimiter.Config, keyFn func(*fiber.Ctx) ratelimiter.Key, opts ...Option) fiber.Handler {
	config := options{}
	for _, opt := range opts {
		if opt != nil {
			opt(&config)
		}
	}

	return func(c *fiber.Ctx) error {
		startedAt := time.Now()
		key := keyFn(c)

		cfgCopy := cfg
		cfgCopy.Key = key

		result, err := limiter.Check(c.UserContext(), cfgCopy)
		if err != nil {
			emitEvent(config.observers, c.UserContext(), cfgCopy, ratelimiter.Result{}, err, time.Since(startedAt))
			if config.errorHandler != nil {
				fakeW := newFiberResponseWriter(c)
				fakeR := newFiberHTTPRequest(c)
				if shouldProceed := config.errorHandler(fakeW, fakeR, err); shouldProceed {
					return c.Next()
				}
				return nil
			}
			return c.Status(http.StatusServiceUnavailable).JSON(fiber.Map{
				"error": "rate limiter unavailable",
			})
		}

		switch result.Status {
		case ratelimiter.Allowed:
			emitEvent(config.observers, c.UserContext(), cfgCopy, result, nil, time.Since(startedAt))
			setFiberHeaders(c, cfgCopy, result, config.standardHeaders)
			c.Response().Header.Set("X-RateLimit-Remaining", strconv.Itoa(result.Remaining))
			c.Response().Header.Set("X-RateLimit-Reset", strconv.FormatInt(result.ResetAt.Unix(), 10))
			return c.Next()

		case ratelimiter.Denied:
			emitEvent(config.observers, c.UserContext(), cfgCopy, result, nil, time.Since(startedAt))
			setFiberHeaders(c, cfgCopy, result, config.standardHeaders)
			if config.deniedHandler != nil {
				fakeW := newFiberResponseWriter(c)
				fakeR := newFiberHTTPRequest(c)
				config.deniedHandler(fakeW, fakeR, result)
				return nil
			}
			retryAfter := int(result.RetryAfter.Seconds())
			c.Response().Header.Set("Retry-After", strconv.Itoa(retryAfter))
			return c.Status(http.StatusTooManyRequests).JSON(fiber.Map{
				"error":       "rate limit exceeded",
				"retry_after": retryAfter,
			})
		}

		return c.Next()
	}
}

// FiberParamKeyFunc derives a rate limit key from a Fiber route parameter.
func FiberParamKeyFunc(param string) func(*fiber.Ctx) ratelimiter.Key {
	return func(c *fiber.Ctx) ratelimiter.Key {
		if c == nil {
			return ""
		}
		return ratelimiter.Key(c.Params(param))
	}
}

// FiberIPKeyFunc derives a rate limit key from the client IP.
func FiberIPKeyFunc() func(*fiber.Ctx) ratelimiter.Key {
	return func(c *fiber.Ctx) ratelimiter.Key {
		if c == nil {
			return ""
		}
		return ratelimiter.Key(c.IP())
	}
}

// FiberHeaderKeyFunc derives a rate limit key from a request header.
func FiberHeaderKeyFunc(header string) func(*fiber.Ctx) ratelimiter.Key {
	return func(c *fiber.Ctx) ratelimiter.Key {
		if c == nil {
			return ""
		}
		return ratelimiter.Key(c.Get(header))
	}
}

func setFiberHeaders(c *fiber.Ctx, cfg ratelimiter.Config, result ratelimiter.Result, enabled bool) {
	if !enabled {
		return
	}

	resetSeconds := int(time.Until(result.ResetAt).Seconds())
	if resetSeconds < 0 {
		resetSeconds = 0
	}

	c.Response().Header.Set("RateLimit-Limit", strconv.Itoa(cfg.Rate))
	c.Response().Header.Set("RateLimit-Remaining", strconv.Itoa(result.Remaining))
	c.Response().Header.Set("RateLimit-Reset", strconv.Itoa(resetSeconds))
}

type fiberResponseWriter struct {
	c      *fiber.Ctx
	header http.Header
	code   int
	body   []byte
}

func newFiberResponseWriter(c *fiber.Ctx) *fiberResponseWriter {
	return &fiberResponseWriter{
		c:      c,
		header: make(http.Header),
		code:   http.StatusOK,
	}
}

func (w *fiberResponseWriter) Header() http.Header        { return w.header }
func (w *fiberResponseWriter) Write(b []byte) (int, error) { w.body = append(w.body, b...); return len(b), nil }
func (w *fiberResponseWriter) WriteHeader(code int)        { w.code = code }

func newFiberHTTPRequest(c *fiber.Ctx) *http.Request {
	req, err := http.NewRequest(c.Method(), c.Path(), nil)
	if err != nil {
		return &http.Request{}
	}
	c.Request().Header.VisitAll(func(key, value []byte) {
		req.Header.Set(string(key), string(value))
	})
	return req
}