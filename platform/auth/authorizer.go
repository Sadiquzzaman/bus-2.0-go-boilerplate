package auth

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/Sadiquzzaman/bus-2.0-go-boilerplate/platform/reqctx"
)

// Authorizer answers permission questions. The IAM service owns the rules;
// its Go client library implements this interface.
//
// Implementations MUST fail closed: on any error (timeout, IAM down) return
// an error, never true.
type Authorizer interface {
	// Can reports whether p may perform permission (e.g. "bus.trip.manage")
	// within scope. scope.CompanyID is the workspace; Vertical/Tenant narrow it.
	Can(ctx context.Context, p Principal, permission string, scope reqctx.Scope) (bool, error)

	// AccessList returns which verticals and tenants of companyID p can see
	// for permission. Services use it to filter list queries.
	AccessList(ctx context.Context, p Principal, permission, companyID string) (AccessList, error)
}

// AccessList is the set of verticals/tenants a caller may see in a company.
type AccessList struct {
	// WholeCompany is true when a role covers the whole company; the
	// vertical and tenant lists are then ignored.
	WholeCompany bool     `json:"whole_company"`
	VerticalIDs  []string `json:"vertical_ids,omitempty"`
	TenantIDs    []string `json:"tenant_ids,omitempty"`
}

// Empty reports whether the caller can see nothing.
func (a AccessList) Empty() bool {
	return !a.WholeCompany && len(a.VerticalIDs) == 0 && len(a.TenantIDs) == 0
}

// Allows reports whether a record in (verticalID, tenantID) is visible.
// A vertical grant covers all its tenants.
func (a AccessList) Allows(verticalID, tenantID string) bool {
	if a.WholeCompany {
		return true
	}
	if verticalID != "" && slices.Contains(a.VerticalIDs, verticalID) {
		return true
	}
	return tenantID != "" && slices.Contains(a.TenantIDs, tenantID)
}

// NewAuthorizer returns the Authorizer selected by mode.
//
//	allow-all  grants everything; refused in production
//	iam        the IAM service client — wire it here once the IAM team
//	           publishes its Go library
func NewAuthorizer(mode, appEnv string) (Authorizer, error) {
	switch mode {
	case "", "allow-all":
		if appEnv == "production" {
			return nil, fmt.Errorf("auth: authorizer allow-all is not allowed in %s", appEnv)
		}
		return AllowAll{}, nil
	case "iam":
		// TODO(iam): return iamclient.New(...) when the IAM Go library exists.
		return nil, errors.New("auth: iam authorizer not wired yet (see platform/auth/authorizer.go)")
	default:
		return nil, fmt.Errorf("auth: unknown authorizer %q", mode)
	}
}

// ErrAuthorizerUnavailable signals the authorizer could not answer.
var ErrAuthorizerUnavailable = errors.New("auth: authorizer unavailable")

// AllowAll grants everything. For local development and tests only;
// NewAuthorizer refuses it in production.
type AllowAll struct{}

func (AllowAll) Can(context.Context, Principal, string, reqctx.Scope) (bool, error) {
	return true, nil
}

func (AllowAll) AccessList(context.Context, Principal, string, string) (AccessList, error) {
	return AccessList{WholeCompany: true}, nil
}

// ScopeOnly allows service tokens with the given OAuth scope and denies
// everything else. Useful for internal-only services that are never called
// by end users.
type ScopeOnly struct{ Scope string }

func (s ScopeOnly) Can(_ context.Context, p Principal, _ string, _ reqctx.Scope) (bool, error) {
	return p.IsService() && p.HasScope(s.Scope), nil
}

func (s ScopeOnly) AccessList(_ context.Context, p Principal, _ string, _ string) (AccessList, error) {
	return AccessList{WholeCompany: p.IsService() && p.HasScope(s.Scope)}, nil
}
