// Package reqctx carries per-request values through context.Context:
// request/correlation IDs, the business scope (company → vertical → tenant)
// and the actor performing the action.
//
// It is a leaf package (stdlib only) so every other platform package can use it.
package reqctx

import "context"

// Standard HTTP / Kafka header names used across all services.
const (
	HeaderRequestID     = "X-Request-ID"
	HeaderCorrelationID = "X-Correlation-ID"
	HeaderCompanyID     = "X-Company-ID"
	HeaderVerticalID    = "X-Vertical-ID"
	HeaderTenantID      = "X-Tenant-ID"
)

// Scope is the business wall a request operates in.
// CompanyID is the workspace; VerticalID and TenantID are optional narrowing.
type Scope struct {
	CompanyID  string `json:"company_id,omitempty"`
	VerticalID string `json:"vertical_id,omitempty"`
	TenantID   string `json:"tenant_id,omitempty"`
}

// IsZero reports whether no scope is set.
func (s Scope) IsZero() bool { return s.CompanyID == "" && s.VerticalID == "" && s.TenantID == "" }

// ActorType says who performed an action.
type ActorType string

const (
	ActorUser    ActorType = "user"
	ActorService ActorType = "service"
	ActorSystem  ActorType = "system"
)

// Actor is the identity behind a request or event.
type Actor struct {
	Type ActorType `json:"type"`
	ID   string    `json:"id"`
}

type (
	requestIDKey     struct{}
	correlationIDKey struct{}
	scopeKey         struct{}
	actorKey         struct{}
	bypassScopeKey   struct{}
)

func WithRequestID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, requestIDKey{}, id)
}

func RequestID(ctx context.Context) string {
	v, _ := ctx.Value(requestIDKey{}).(string)
	return v
}

func WithCorrelationID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, correlationIDKey{}, id)
}

// CorrelationID returns the correlation ID, falling back to the request ID.
func CorrelationID(ctx context.Context) string {
	if v, _ := ctx.Value(correlationIDKey{}).(string); v != "" {
		return v
	}
	return RequestID(ctx)
}

func WithScope(ctx context.Context, s Scope) context.Context {
	return context.WithValue(ctx, scopeKey{}, s)
}

func ScopeFrom(ctx context.Context) (Scope, bool) {
	s, ok := ctx.Value(scopeKey{}).(Scope)
	return s, ok && !s.IsZero()
}

func WithActor(ctx context.Context, a Actor) context.Context {
	return context.WithValue(ctx, actorKey{}, a)
}

func ActorFrom(ctx context.Context) (Actor, bool) {
	a, ok := ctx.Value(actorKey{}).(Actor)
	return a, ok
}

// WithoutScopeFilter marks ctx so automatic company scoping (ent interceptor,
// mongo helpers) is skipped. Use only for system jobs: outbox relay,
// migrations, cross-company admin reports. Never from request handlers.
func WithoutScopeFilter(ctx context.Context) context.Context {
	return context.WithValue(ctx, bypassScopeKey{}, true)
}

// ScopeFilterDisabled reports whether WithoutScopeFilter was applied.
func ScopeFilterDisabled(ctx context.Context) bool {
	v, _ := ctx.Value(bypassScopeKey{}).(bool)
	return v
}

// Detach returns a fresh background context carrying the same request values.
// Use it for work that must outlive the request (e.g. async publishing).
func Detach(ctx context.Context) context.Context {
	out := context.Background()
	if v := RequestID(ctx); v != "" {
		out = WithRequestID(out, v)
	}
	if v, _ := ctx.Value(correlationIDKey{}).(string); v != "" {
		out = WithCorrelationID(out, v)
	}
	if s, ok := ScopeFrom(ctx); ok {
		out = WithScope(out, s)
	}
	if a, ok := ActorFrom(ctx); ok {
		out = WithActor(out, a)
	}
	return out
}
