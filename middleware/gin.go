package middleware

import (
	"net/http"

	"github.com/gin-gonic/gin"
	ratelimiter "github.com/seanrobmerriam/rate-limiter-go"
)

func Gin(limiter ratelimiter.Limiter, cfg ratelimiter.Config, keyFn func(*gin.Context) ratelimiter.Key, opts ...Option) gin.HandlerFunc {
	return func(c *gin.Context) {
		wrapped := MiddlewareWithOptions(
			limiter,
			cfg,
			func(r *http.Request) ratelimiter.Key {
				if c == nil || keyFn == nil {
					return ""
				}
				return keyFn(c)
			},
			opts...,
		)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			c.Request = r
			c.Next()
		}))

		wrapped.ServeHTTP(c.Writer, c.Request)
	}
}

func GinParamKeyFunc(param string) func(*gin.Context) ratelimiter.Key {
	return func(c *gin.Context) ratelimiter.Key {
		if c == nil {
			return ""
		}
		return ratelimiter.Key(c.Param(param))
	}
}
