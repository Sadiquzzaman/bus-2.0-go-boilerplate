package httpserver

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"runtime/debug"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/Sadiquzzaman/bus-2.0-go-boilerplate/platform/apperror"
	"github.com/Sadiquzzaman/bus-2.0-go-boilerplate/platform/reqctx"
)

var (
	errNotFoundRoute    = apperror.NotFound("route not found")
	errMethodNotAllowed = apperror.New("METHOD_NOT_ALLOWED", http.StatusMethodNotAllowed, "method not allowed")
)

// setCtx replaces the request context.
func setCtx(c *gin.Context, ctx context.Context) { c.Request = c.Request.WithContext(ctx) }

// RequestID reads or creates X-Request-ID and X-Correlation-ID and puts
// them in the request context and response headers.
func RequestID() gin.HandlerFunc {
	return func(c *gin.Context) {
		rid := sanitizeID(c.GetHeader(reqctx.HeaderRequestID))
		if rid == "" {
			rid = uuid.Must(uuid.NewV7()).String()
		}
		cid := sanitizeID(c.GetHeader(reqctx.HeaderCorrelationID))
		if cid == "" {
			cid = rid
		}
		ctx := reqctx.WithRequestID(c.Request.Context(), rid)
		ctx = reqctx.WithCorrelationID(ctx, cid)
		setCtx(c, ctx)
		c.Header(reqctx.HeaderRequestID, rid)
		c.Header(reqctx.HeaderCorrelationID, cid)
		c.Next()
	}
}

// sanitizeID accepts client-supplied IDs only if short and printable.
func sanitizeID(s string) string {
	if s == "" || len(s) > 128 {
		return ""
	}
	for _, r := range s {
		if r < 0x21 || r > 0x7e {
			return ""
		}
	}
	return s
}

// Recovery turns panics into 500 responses and logs the stack.
func Recovery(log *slog.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		defer func() {
			if rec := recover(); rec != nil {
				log.ErrorContext(c.Request.Context(), "panic recovered",
					slog.String("panic", fmt.Sprint(rec)),
					slog.String("stack", string(debug.Stack())),
				)
				if !c.Writer.Written() {
					Fail(c, apperror.Internal("internal error"))
				}
				c.Abort()
			}
		}()
		c.Next()
	}
}

// AccessLog writes one structured line per request.
func AccessLog(log *slog.Logger, skipPaths ...string) gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		c.Next()
		if slices.Contains(skipPaths, c.Request.URL.Path) {
			return
		}
		status := c.Writer.Status()
		level := slog.LevelInfo
		switch {
		case status >= 500:
			level = slog.LevelError
		case status >= 400:
			level = slog.LevelWarn
		}
		attrs := []slog.Attr{
			slog.String("method", c.Request.Method),
			slog.String("route", c.FullPath()),
			slog.String("path", c.Request.URL.Path),
			slog.Int("status", status),
			slog.Int64("duration_ms", time.Since(start).Milliseconds()),
			slog.String("client_ip", c.ClientIP()),
			slog.String("user_agent", c.Request.UserAgent()),
			slog.Int("bytes", c.Writer.Size()),
		}
		if errs := c.Errors.ByType(gin.ErrorTypeAny); len(errs) > 0 {
			attrs = append(attrs, slog.String("error", errs.String()))
		}
		log.LogAttrs(c.Request.Context(), level, "http request", attrs...)
	}
}

// Timeout puts a deadline on the request context. Handlers and every
// downstream call (DB, Redis, HTTP) must pass c.Request.Context() along.
func Timeout(d time.Duration) gin.HandlerFunc {
	return func(c *gin.Context) {
		if d <= 0 {
			c.Next()
			return
		}
		ctx, cancel := context.WithTimeout(c.Request.Context(), d)
		defer cancel()
		setCtx(c, ctx)
		c.Next()
	}
}

// BodyLimit caps request body size.
func BodyLimit(n int64) gin.HandlerFunc {
	return func(c *gin.Context) {
		if n > 0 && c.Request.Body != nil {
			c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, n)
		}
		c.Next()
	}
}

// CORS allows the configured origins. Usually the edge gateway handles CORS;
// leave CORS_ALLOWED_ORIGINS empty in that case. "*" allows any origin.
func CORS(origins []string) gin.HandlerFunc {
	allowAll := slices.Contains(origins, "*")
	return func(c *gin.Context) {
		origin := c.GetHeader("Origin")
		if origin == "" || len(origins) == 0 || (!allowAll && !slices.Contains(origins, origin)) {
			c.Next()
			return
		}
		h := c.Writer.Header()
		h.Set("Access-Control-Allow-Origin", origin)
		h.Add("Vary", "Origin")
		h.Set("Access-Control-Allow-Credentials", "true")
		h.Set("Access-Control-Expose-Headers", strings.Join([]string{reqctx.HeaderRequestID, reqctx.HeaderCorrelationID}, ", "))
		if c.Request.Method == http.MethodOptions {
			h.Set("Access-Control-Allow-Methods", "GET, POST, PUT, PATCH, DELETE, OPTIONS")
			h.Set("Access-Control-Allow-Headers", strings.Join([]string{
				"Authorization", "Content-Type", "Idempotency-Key",
				reqctx.HeaderRequestID, reqctx.HeaderCorrelationID,
				reqctx.HeaderCompanyID, reqctx.HeaderVerticalID, reqctx.HeaderTenantID,
			}, ", "))
			h.Set("Access-Control-Max-Age", "600")
			c.AbortWithStatus(http.StatusNoContent)
			return
		}
		c.Next()
	}
}

var (
	httpDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "http_server_request_duration_seconds",
		Help:    "HTTP request latency.",
		Buckets: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
	}, []string{"service", "method", "route", "status"})
	httpInflight = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "http_server_requests_in_flight",
		Help: "HTTP requests currently being served.",
	}, []string{"service"})
)

// Metrics records Prometheus request metrics, labelled by route template.
func Metrics(service string) gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		g := httpInflight.WithLabelValues(service)
		g.Inc()
		c.Next()
		g.Dec()
		route := c.FullPath()
		if route == "" {
			route = "unmatched"
		}
		httpDuration.WithLabelValues(service, c.Request.Method, route, strconv.Itoa(c.Writer.Status())).
			Observe(time.Since(start).Seconds())
	}
}
