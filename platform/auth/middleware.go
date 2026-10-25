package auth

import (
	"context"
	"errors"
	"log/slog"
	"strings"

	"github.com/gin-gonic/gin"

	"github.com/Sadiquzzaman/bus-2.0-go-boilerplate/platform/apperror"
	"github.com/Sadiquzzaman/bus-2.0-go-boilerplate/platform/httpserver"
	"github.com/Sadiquzzaman/bus-2.0-go-boilerplate/platform/reqctx"
)

// Authenticate requires a valid bearer token (401 otherwise) and stores the
// Principal and Actor in the request context.
func Authenticate(v Verifier) gin.HandlerFunc {
	return func(c *gin.Context) {
		ctx := c.Request.Context()
		if _, dev := v.(devVerifier); dev {
			ctx = context.WithValue(ctx, devUserKey{}, c.GetHeader(DevUserHeader))
		}
		p, err := v.Verify(ctx, bearer(c.GetHeader("Authorization")))
		if err != nil {
			slog.DebugContext(ctx, "authentication failed", slog.String("error", err.Error()))
			msg := "invalid or expired token"
			if errors.Is(err, ErrMissingToken) {
				msg = "missing bearer token"
			}
			c.Header("WWW-Authenticate", `Bearer error="invalid_token"`)
			httpserver.Fail(c, apperror.Unauthorized(msg))
			return
		}
		c.Request = c.Request.WithContext(WithPrincipal(c.Request.Context(), p))
		c.Next()
	}
}

func bearer(h string) string {
	const prefix = "bearer "
	if len(h) > len(prefix) && strings.EqualFold(h[:len(prefix)], prefix) {
		return strings.TrimSpace(h[len(prefix):])
	}
	return ""
}

// RequirePermission checks permission in the request's scope (set by
// httpserver.Scope). 403 when denied, 503 when the authorizer cannot answer.
// It never allows by default.
func RequirePermission(a Authorizer, permission string) gin.HandlerFunc {
	return func(c *gin.Context) {
		ctx := c.Request.Context()
		p, ok := PrincipalFrom(ctx)
		if !ok {
			httpserver.Fail(c, apperror.Unauthorized("not authenticated"))
			return
		}
		scope, _ := reqctx.ScopeFrom(ctx)
		allowed, err := a.Can(ctx, p, permission, scope)
		if err != nil {
			httpserver.Fail(c, apperror.Unavailable("authorization unavailable").Wrap(err))
			return
		}
		if !allowed {
			httpserver.Fail(c, apperror.Forbidden("missing permission "+permission))
			return
		}
		c.Next()
	}
}

// RequireScope requires an OAuth scope on the token (e.g. for internal
// service-to-service endpoints).
func RequireScope(scope string) gin.HandlerFunc {
	return func(c *gin.Context) {
		p, ok := PrincipalFrom(c.Request.Context())
		if !ok || !(p.HasScope(scope) || p.HasScope("*")) {
			httpserver.Fail(c, apperror.Forbidden("missing scope "+scope))
			return
		}
		c.Next()
	}
}

type accessListKey struct{}

// LoadAccessList resolves the caller's visible verticals/tenants for
// permission in the current company and stores it in the context, for list
// endpoints. Read it with AccessListFrom.
func LoadAccessList(a Authorizer, permission string) gin.HandlerFunc {
	return func(c *gin.Context) {
		ctx := c.Request.Context()
		p, ok := PrincipalFrom(ctx)
		if !ok {
			httpserver.Fail(c, apperror.Unauthorized("not authenticated"))
			return
		}
		scope, ok := reqctx.ScopeFrom(ctx)
		if !ok || scope.CompanyID == "" {
			httpserver.Fail(c, apperror.BadRequest(reqctx.HeaderCompanyID+" header is required"))
			return
		}
		al, err := a.AccessList(ctx, p, permission, scope.CompanyID)
		if err != nil {
			httpserver.Fail(c, apperror.Unavailable("authorization unavailable").Wrap(err))
			return
		}
		if al.Empty() {
			httpserver.Fail(c, apperror.Forbidden("missing permission "+permission))
			return
		}
		c.Request = c.Request.WithContext(context.WithValue(ctx, accessListKey{}, al))
		c.Next()
	}
}

// AccessListFrom returns the list stored by LoadAccessList.
func AccessListFrom(ctx context.Context) (AccessList, bool) {
	al, ok := ctx.Value(accessListKey{}).(AccessList)
	return al, ok
}
