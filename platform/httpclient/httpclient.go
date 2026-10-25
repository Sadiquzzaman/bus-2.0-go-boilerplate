// Package httpclient is the client for synchronous service-to-service reads
// (writes between services go through Kafka).
//
// Every call:
//   - carries X-Request-ID, X-Correlation-ID, the scope headers and the W3C
//     trace context of the incoming request;
//   - authenticates with the calling service's own client-credentials token
//     (cached and refreshed), and names the end user in X-On-Behalf-Of;
//   - has a timeout, retries idempotent requests on 429/502/503/504 and
//     network errors with jittered backoff, and goes through a per-target
//     circuit breaker;
//   - turns error responses back into *apperror.Error.
package httpclient

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/sony/gobreaker/v2"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"

	"github.com/Sadiquzzaman/bus-2.0-go-boilerplate/platform/apperror"
	"github.com/Sadiquzzaman/bus-2.0-go-boilerplate/platform/reqctx"
)

// HeaderOnBehalfOf carries the end-user ID on service-token calls.
const HeaderOnBehalfOf = "X-On-Behalf-Of"

// Config tunes resilience. Env prefix chosen by the embedding struct.
type Config struct {
	Timeout            time.Duration `env:"TIMEOUT" envDefault:"5s"`
	MaxRetries         int           `env:"MAX_RETRIES" envDefault:"2"`
	RetryBaseDelay     time.Duration `env:"RETRY_BASE_DELAY" envDefault:"100ms"`
	BreakerFailures    uint32        `env:"BREAKER_FAILURES" envDefault:"5"`
	BreakerOpenTimeout time.Duration `env:"BREAKER_OPEN_TIMEOUT" envDefault:"30s"`
}

// CredentialsConfig is this service's OAuth client (client-credentials grant).
type CredentialsConfig struct {
	TokenURL     string   `env:"TOKEN_URL"`
	ClientID     string   `env:"CLIENT_ID"`
	ClientSecret string   `env:"CLIENT_SECRET"`
	Scopes       []string `env:"SCOPES" envSeparator:" "`
}

// TokenSource returns a cached, auto-refreshing token source, or nil when
// TokenURL is empty (local development without an identity provider).
func (c CredentialsConfig) TokenSource(audience string) oauth2.TokenSource {
	if c.TokenURL == "" {
		return nil
	}
	cc := clientcredentials.Config{
		ClientID:     c.ClientID,
		ClientSecret: c.ClientSecret,
		TokenURL:     c.TokenURL,
		Scopes:       c.Scopes,
	}
	if audience != "" {
		cc.EndpointParams = url.Values{"audience": {audience}}
	}
	return oauth2.ReuseTokenSource(nil, cc.TokenSource(context.Background()))
}

// Client calls one target service.
type Client struct {
	name    string
	baseURL string
	cfg     Config
	hc      *http.Client
	tokens  oauth2.TokenSource
	breaker *gobreaker.CircuitBreaker[*http.Response]
}

// New creates a client for the service `name` at baseURL
// (e.g. "http://trip-service.bus.svc.cluster.local:8080").
// tokens may be nil to send no Authorization header.
func New(name, baseURL string, cfg Config, tokens oauth2.TokenSource) *Client {
	return &Client{
		name:    name,
		baseURL: strings.TrimRight(baseURL, "/"),
		cfg:     cfg,
		tokens:  tokens,
		hc: &http.Client{
			Timeout:   cfg.Timeout,
			Transport: otelhttp.NewTransport(http.DefaultTransport),
		},
		breaker: gobreaker.NewCircuitBreaker[*http.Response](gobreaker.Settings{
			Name:        name,
			MaxRequests: 1,
			Timeout:     cfg.BreakerOpenTimeout,
			ReadyToTrip: func(c gobreaker.Counts) bool { return c.ConsecutiveFailures >= cfg.BreakerFailures },
		}),
	}
}

// GetJSON performs GET baseURL+path?query and decodes the "data" field of the
// standard response envelope into out.
func (c *Client) GetJSON(ctx context.Context, path string, query url.Values, out any) error {
	u := c.baseURL + path
	if len(query) > 0 {
		u += "?" + query.Encode()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return err
	}
	resp, err := c.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return decodeData(resp.Body, out)
}

// Do sends req with headers, auth, retries and the breaker applied.
// Non-2xx responses are returned as *apperror.Error.
func (c *Client) Do(req *http.Request) (*http.Response, error) {
	ctx := req.Context()
	if err := c.decorate(req); err != nil {
		return nil, err
	}

	idempotent := req.Method == http.MethodGet || req.Method == http.MethodHead
	var body []byte
	if req.Body != nil && idempotent {
		body, _ = io.ReadAll(req.Body)
		req.Body.Close()
	}

	attempts := 1
	if idempotent {
		attempts += c.cfg.MaxRetries
	}

	var lastErr error
	for i := 0; i < attempts; i++ {
		if i > 0 {
			if err := sleep(ctx, backoff(c.cfg.RetryBaseDelay, i)); err != nil {
				return nil, err
			}
		}
		r := req.Clone(ctx)
		if body != nil {
			r.Body = io.NopCloser(bytes.NewReader(body))
		}
		resp, err := c.breaker.Execute(func() (*http.Response, error) {
			resp, err := c.hc.Do(r)
			if err != nil {
				return nil, err
			}
			if resp.StatusCode >= 500 {
				// Count 5xx as breaker failures but still hand back the body.
				return resp, errUpstream5xx
			}
			return resp, nil
		})
		switch {
		case errors.Is(err, gobreaker.ErrOpenState), errors.Is(err, gobreaker.ErrTooManyRequests):
			return nil, apperror.Unavailable(c.name + " unavailable (circuit open)").Wrap(err)
		case err != nil && resp == nil:
			lastErr = apperror.Unavailable(c.name + " unreachable").Wrap(err)
			if ctx.Err() != nil {
				return nil, lastErr
			}
			continue
		}
		if retryable(resp.StatusCode) && i < attempts-1 {
			drain(resp)
			lastErr = apperror.Unavailable(fmt.Sprintf("%s returned %d", c.name, resp.StatusCode))
			continue
		}
		if resp.StatusCode >= 400 {
			defer resp.Body.Close()
			return nil, toAppError(c.name, resp)
		}
		return resp, nil
	}
	return nil, lastErr
}

var errUpstream5xx = errors.New("upstream 5xx")

func (c *Client) decorate(req *http.Request) error {
	ctx := req.Context()
	h := req.Header
	if v := reqctx.RequestID(ctx); v != "" {
		h.Set(reqctx.HeaderRequestID, v)
	}
	if v := reqctx.CorrelationID(ctx); v != "" {
		h.Set(reqctx.HeaderCorrelationID, v)
	}
	if s, ok := reqctx.ScopeFrom(ctx); ok {
		setIf(h, reqctx.HeaderCompanyID, s.CompanyID)
		setIf(h, reqctx.HeaderVerticalID, s.VerticalID)
		setIf(h, reqctx.HeaderTenantID, s.TenantID)
	}
	if a, ok := reqctx.ActorFrom(ctx); ok && a.Type == reqctx.ActorUser {
		h.Set(HeaderOnBehalfOf, a.ID)
	}
	h.Set("Accept", "application/json")
	if c.tokens != nil {
		tok, err := c.tokens.Token()
		if err != nil {
			return apperror.Unavailable("cannot obtain service token").Wrap(err)
		}
		tok.SetAuthHeader(req)
	}
	return nil
}

func setIf(h http.Header, k, v string) {
	if v != "" {
		h.Set(k, v)
	}
}

func retryable(status int) bool {
	return status == http.StatusTooManyRequests || status == http.StatusBadGateway ||
		status == http.StatusServiceUnavailable || status == http.StatusGatewayTimeout
}

func backoff(base time.Duration, attempt int) time.Duration {
	d := base << (attempt - 1)
	return d/2 + time.Duration(rand.Int64N(int64(d/2)+1))
}

func sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

func drain(resp *http.Response) {
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
	resp.Body.Close()
}

func decodeData(r io.Reader, out any) error {
	if out == nil {
		return nil
	}
	var env struct {
		Data json.RawMessage `json:"data"`
	}
	if err := json.NewDecoder(r).Decode(&env); err != nil {
		return apperror.Internal("invalid upstream response").Wrap(err)
	}
	if err := json.Unmarshal(env.Data, out); err != nil {
		return apperror.Internal("invalid upstream data").Wrap(err)
	}
	return nil
}

func toAppError(name string, resp *http.Response) error {
	var body struct {
		Error struct {
			Code    apperror.Code `json:"code"`
			Message string        `json:"message"`
			Details any           `json:"details"`
		} `json:"error"`
	}
	_ = json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&body)
	if resp.StatusCode >= 500 {
		return apperror.Unavailable(fmt.Sprintf("%s failed with %d", name, resp.StatusCode))
	}
	code := body.Error.Code
	if code == "" {
		code = apperror.Code(fmt.Sprintf("UPSTREAM_%d", resp.StatusCode))
	}
	msg := body.Error.Message
	if msg == "" {
		msg = http.StatusText(resp.StatusCode)
	}
	return apperror.New(code, resp.StatusCode, msg).WithDetails(body.Error.Details)
}
