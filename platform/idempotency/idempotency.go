// Package idempotency makes unsafe HTTP requests (POST/PUT/PATCH/DELETE)
// safe to retry. A client sends `Idempotency-Key: <uuid>`; the first request
// runs and its response is stored in Redis for TTL; repeats with the same key
// (same caller, same route) get the stored response replayed. A repeat that
// arrives while the first is still running gets 409.
package idempotency

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/Sadiquzzaman/bus-2.0-go-boilerplate/platform/apperror"
	"github.com/Sadiquzzaman/bus-2.0-go-boilerplate/platform/auth"
	"github.com/Sadiquzzaman/bus-2.0-go-boilerplate/platform/httpserver"
	"github.com/Sadiquzzaman/bus-2.0-go-boilerplate/platform/redis"
	"github.com/Sadiquzzaman/bus-2.0-go-boilerplate/platform/reqctx"
)

// Header is the request header clients send.
const Header = "Idempotency-Key"

// Options for the middleware.
type Options struct {
	TTL      time.Duration // how long results are kept (default 24h)
	Required bool          // reject requests without the header
}

type stored struct {
	State       string            `json:"state"` // "running" | "done"
	Status      int               `json:"status,omitempty"`
	Body        []byte            `json:"body,omitempty"`
	ContentType string            `json:"content_type,omitempty"`
	Headers     map[string]string `json:"headers,omitempty"`
}

// Middleware returns the gin middleware.
func Middleware(rdb *redis.Client, opts Options) gin.HandlerFunc {
	if opts.TTL <= 0 {
		opts.TTL = 24 * time.Hour
	}
	return func(c *gin.Context) {
		switch c.Request.Method {
		case http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete:
		default:
			c.Next()
			return
		}
		key := c.GetHeader(Header)
		if key == "" {
			if opts.Required {
				httpserver.Fail(c, apperror.BadRequest(Header+" header is required"))
				return
			}
			c.Next()
			return
		}
		if len(key) > 128 {
			httpserver.Fail(c, apperror.BadRequest(Header+" is too long"))
			return
		}

		ctx := c.Request.Context()
		rkey := rdb.Key("idem", fingerprint(ctx, c, key))

		// Claim the key.
		ok, err := rdb.SetNX(ctx, rkey, mustJSON(stored{State: "running"}), opts.TTL).Result()
		if err != nil {
			httpserver.Fail(c, apperror.Unavailable("idempotency store unavailable").Wrap(err))
			return
		}
		if !ok {
			var prev stored
			found, err := rdb.GetJSON(ctx, rkey, &prev)
			switch {
			case err != nil:
				httpserver.Fail(c, apperror.Unavailable("idempotency store unavailable").Wrap(err))
			case !found || prev.State == "running":
				httpserver.Fail(c, apperror.Conflict("a request with this "+Header+" is already in progress"))
			default:
				for k, v := range prev.Headers {
					c.Header(k, v)
				}
				c.Header("Idempotent-Replayed", "true")
				c.Data(prev.Status, prev.ContentType, prev.Body)
				c.Abort()
			}
			return
		}

		rec := &recorder{ResponseWriter: c.Writer}
		c.Writer = rec
		c.Next()

		// Do not cache server errors: let the client retry for real.
		bg := reqctx.Detach(ctx)
		if rec.Status() >= 500 {
			_ = rdb.Del(bg, rkey).Err()
			return
		}
		_ = rdb.SetJSON(bg, rkey, stored{
			State:       "done",
			Status:      rec.Status(),
			Body:        rec.buf.Bytes(),
			ContentType: rec.Header().Get("Content-Type"),
			Headers:     map[string]string{"Location": rec.Header().Get("Location")},
		}, opts.TTL)
	}
}

// fingerprint binds the key to caller and route so two users (or two
// endpoints) can never share a stored response.
func fingerprint(ctx context.Context, c *gin.Context, key string) string {
	caller := "anonymous"
	if p, ok := auth.PrincipalFrom(ctx); ok {
		caller = p.Subject
	}
	h := sha256.Sum256([]byte(caller + "|" + c.Request.Method + "|" + c.FullPath() + "|" + key))
	return hex.EncodeToString(h[:])
}

type recorder struct {
	gin.ResponseWriter
	buf bytes.Buffer
}

func (r *recorder) Write(b []byte) (int, error) {
	r.buf.Write(b)
	return r.ResponseWriter.Write(b)
}

func (r *recorder) WriteString(s string) (int, error) {
	r.buf.WriteString(s)
	return r.ResponseWriter.WriteString(s)
}
