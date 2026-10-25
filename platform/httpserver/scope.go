package httpserver

import (
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"github.com/Sadiquzzaman/bus-2.0-go-boilerplate/platform/apperror"
	"github.com/Sadiquzzaman/bus-2.0-go-boilerplate/platform/reqctx"
)

// Scope reads the workspace headers (X-Company-ID, X-Vertical-ID,
// X-Tenant-ID) into the request context. Values must be UUIDs.
//
// Headers only say what the caller *wants* to act on. Whether the caller is
// *allowed* to is decided by auth.RequirePermission, which must run after
// this middleware.
func Scope() gin.HandlerFunc {
	return func(c *gin.Context) {
		s := reqctx.Scope{
			CompanyID:  c.GetHeader(reqctx.HeaderCompanyID),
			VerticalID: c.GetHeader(reqctx.HeaderVerticalID),
			TenantID:   c.GetHeader(reqctx.HeaderTenantID),
		}
		var bad []apperror.FieldError
		for name, v := range map[string]string{
			reqctx.HeaderCompanyID:  s.CompanyID,
			reqctx.HeaderVerticalID: s.VerticalID,
			reqctx.HeaderTenantID:   s.TenantID,
		} {
			if v == "" {
				continue
			}
			if _, err := uuid.Parse(v); err != nil {
				bad = append(bad, apperror.FieldError{Field: name, Message: "must be a valid UUID"})
			}
		}
		if len(bad) > 0 {
			Fail(c, apperror.Validation(bad...))
			return
		}
		if s.CompanyID == "" && (s.VerticalID != "" || s.TenantID != "") {
			Fail(c, apperror.BadRequest(reqctx.HeaderCompanyID+" is required when vertical or tenant is set"))
			return
		}
		if !s.IsZero() {
			setCtx(c, reqctx.WithScope(c.Request.Context(), s))
		}
		c.Next()
	}
}

// RequireCompany rejects requests without X-Company-ID. Use on routes that
// operate inside a workspace (most operator/admin routes).
func RequireCompany() gin.HandlerFunc {
	return func(c *gin.Context) {
		if s, ok := reqctx.ScopeFrom(c.Request.Context()); !ok || s.CompanyID == "" {
			Fail(c, apperror.BadRequest(reqctx.HeaderCompanyID+" header is required"))
			return
		}
		c.Next()
	}
}
