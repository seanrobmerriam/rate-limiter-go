package middleware

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	ratelimiter "github.com/seanrobmerriam/rate-limiter-go"
	"github.com/stretchr/testify/assert"
)

func TestGin_UsesMiddlewareOptions(t *testing.T) {
	gin.SetMode(gin.TestMode)

	limiter := &mockLimiter{
		checkFunc: func(ctx context.Context, cfg ratelimiter.Config) (ratelimiter.Result, error) {
			return ratelimiter.Result{
				Status:    ratelimiter.Allowed,
				Remaining: 1,
				ResetAt:   time.Now().Add(2 * time.Second),
			}, nil
		},
	}

	router := gin.New()
	router.Use(Gin(
		limiter,
		ratelimiter.Config{Algorithm: ratelimiter.TokenBucket, Rate: 2, BurstSize: 2, Window: 2 * time.Second},
		GinParamKeyFunc("accountID"),
		WithStandardHeaders(),
	))
	router.GET("/accounts/:accountID", func(c *gin.Context) {
		c.String(http.StatusOK, "ok")
	})

	req := httptest.NewRequest(http.MethodGet, "/accounts/abc123", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "1", w.Header().Get("X-RateLimit-Remaining"))
	assert.Equal(t, "2", w.Header().Get("RateLimit-Limit"))
	assert.Equal(t, "1", w.Header().Get("RateLimit-Remaining"))
	assert.Equal(t, "ok", w.Body.String())
}

func TestGinParamKeyFunc(t *testing.T) {
	gin.SetMode(gin.TestMode)

	router := gin.New()
	var got ratelimiter.Key

	router.GET("/teams/:teamID", func(c *gin.Context) {
		got = GinParamKeyFunc("teamID")(c)
		c.Status(http.StatusNoContent)
	})

	req := httptest.NewRequest(http.MethodGet, "/teams/red", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, ratelimiter.Key("red"), got)
}
