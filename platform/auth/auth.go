// Package auth authenticates requests (JWT access tokens checked locally
// against the identity provider's JWKS) and authorizes them through a
// pluggable Authorizer.
//
// Authentication (who is calling) is generic and lives here.
// Authorization (may they do X in company/vertical/tenant Y) is answered by
// the IAM service; this package only defines the Authorizer interface so
// services do not depend on how IAM implements it. Plug in IAM's client
// library when it is available.
package auth

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/MicahParks/keyfunc/v3"
	"github.com/golang-jwt/jwt/v5"

	"github.com/Sadiquzzaman/bus-2.0-go-boilerplate/platform/reqctx"
)

// Config for token verification.
type Config struct {
	// Disable only for local development; refused in production.
	Enabled    bool          `env:"ENABLED" envDefault:"true"`
	JWKSURL    string        `env:"JWKS_URL"`
	Issuer     string        `env:"ISSUER"`
	Audience   string        `env:"AUDIENCE"` // this API's audience, e.g. "bus-api"
	Leeway     time.Duration `env:"LEEWAY" envDefault:"30s"`
	Algorithms []string      `env:"ALGORITHMS" envSeparator:"," envDefault:"RS256,ES256"`
	// Which Authorizer to use: "allow-all" (local/dev only) or "iam".
	Authorizer string `env:"AUTHORIZER" envDefault:"allow-all"`
}

// Validate checks required fields when auth is enabled.
func (c Config) Validate() error {
	if !c.Enabled {
		return nil
	}
	var missing []string
	if c.JWKSURL == "" {
		missing = append(missing, "AUTH_JWKS_URL")
	}
	if c.Issuer == "" {
		missing = append(missing, "AUTH_ISSUER")
	}
	if c.Audience == "" {
		missing = append(missing, "AUTH_AUDIENCE")
	}
	if len(missing) > 0 {
		return fmt.Errorf("auth: missing %s", strings.Join(missing, ", "))
	}
	return nil
}

// Principal is the authenticated caller.
type Principal struct {
	Subject  string         // user ID, or client ID for service tokens
	ClientID string         // OAuth client that obtained the token
	Scopes   []string       // OAuth scopes granted to the client
	Token    string         // raw bearer token (forwarded on user-context calls)
	Claims   map[string]any // all claims, for service-specific needs
}

// IsService reports whether the token was issued to a service with the
// client-credentials grant (no end user).
func (p Principal) IsService() bool { return p.Subject != "" && p.Subject == p.ClientID }

// HasScope reports whether the token carries the OAuth scope.
func (p Principal) HasScope(s string) bool { return slices.Contains(p.Scopes, s) }

// Actor converts the principal for logs and events.
func (p Principal) Actor() reqctx.Actor {
	if p.IsService() {
		return reqctx.Actor{Type: reqctx.ActorService, ID: p.ClientID}
	}
	return reqctx.Actor{Type: reqctx.ActorUser, ID: p.Subject}
}

type principalKey struct{}

// WithPrincipal stores p in ctx (and the derived actor).
func WithPrincipal(ctx context.Context, p Principal) context.Context {
	ctx = context.WithValue(ctx, principalKey{}, p)
	return reqctx.WithActor(ctx, p.Actor())
}

// PrincipalFrom returns the authenticated caller, if any.
func PrincipalFrom(ctx context.Context) (Principal, bool) {
	p, ok := ctx.Value(principalKey{}).(Principal)
	return p, ok
}

// Errors returned by Verify.
var (
	ErrMissingToken = errors.New("auth: missing bearer token")
	ErrInvalidToken = errors.New("auth: invalid token")
)

// Verifier validates a raw bearer token.
type Verifier interface {
	Verify(ctx context.Context, token string) (Principal, error)
}

// NewVerifier builds the JWKS verifier, or the dev verifier when auth is
// disabled. Disabling auth in production is an error.
func NewVerifier(ctx context.Context, cfg Config, appEnv string) (Verifier, error) {
	if !cfg.Enabled {
		if appEnv == "production" || appEnv == "staging" {
			return nil, fmt.Errorf("auth: AUTH_ENABLED=false is not allowed in %s", appEnv)
		}
		return devVerifier{}, nil
	}
	return NewJWKSVerifier(ctx, cfg)
}

// JWKSVerifier validates JWTs against a remote JWKS (e.g. Ory Hydra's
// /.well-known/jwks.json). Keys are cached and refreshed in the background.
type JWKSVerifier struct {
	kf   jwt.Keyfunc
	opts []jwt.ParserOption
}

// NewJWKSVerifier fetches the JWKS once and keeps it fresh until ctx ends.
func NewJWKSVerifier(ctx context.Context, cfg Config) (*JWKSVerifier, error) {
	k, err := keyfunc.NewDefaultCtx(ctx, []string{cfg.JWKSURL})
	if err != nil {
		return nil, fmt.Errorf("auth: jwks %s: %w", cfg.JWKSURL, err)
	}
	return &JWKSVerifier{
		kf: k.Keyfunc,
		opts: []jwt.ParserOption{
			jwt.WithIssuer(cfg.Issuer),
			jwt.WithAudience(cfg.Audience),
			jwt.WithLeeway(cfg.Leeway),
			jwt.WithValidMethods(cfg.Algorithms),
			jwt.WithExpirationRequired(),
		},
	}, nil
}

type tokenClaims struct {
	jwt.RegisteredClaims
	ClientID string   `json:"client_id"`
	Scp      []string `json:"scp"`   // Hydra style
	Scope    string   `json:"scope"` // RFC 8693 style, space separated
}

// Verify implements Verifier.
func (v *JWKSVerifier) Verify(_ context.Context, raw string) (Principal, error) {
	if raw == "" {
		return Principal{}, ErrMissingToken
	}
	var c tokenClaims
	if _, err := jwt.ParseWithClaims(raw, &c, v.kf, v.opts...); err != nil {
		return Principal{}, fmt.Errorf("%w: %v", ErrInvalidToken, err)
	}
	if c.Subject == "" {
		return Principal{}, fmt.Errorf("%w: missing sub", ErrInvalidToken)
	}
	// Also decode into a map for service-specific claims.
	all := jwt.MapClaims{}
	_, _, _ = jwt.NewParser().ParseUnverified(raw, all)

	scopes := c.Scp
	if len(scopes) == 0 && c.Scope != "" {
		scopes = strings.Fields(c.Scope)
	}
	return Principal{
		Subject:  c.Subject,
		ClientID: c.ClientID,
		Scopes:   scopes,
		Token:    raw,
		Claims:   all,
	}, nil
}

// devVerifier accepts any request in local development. The caller is taken
// from the X-Dev-User-ID header (default "dev-user").
type devVerifier struct{}

// DevUserHeader names the header the dev verifier reads.
const DevUserHeader = "X-Dev-User-ID"

func (devVerifier) Verify(ctx context.Context, _ string) (Principal, error) {
	id, _ := ctx.Value(devUserKey{}).(string)
	if id == "" {
		id = "dev-user"
	}
	return Principal{Subject: id, ClientID: "dev-client", Scopes: []string{"*"}}, nil
}

type devUserKey struct{}
