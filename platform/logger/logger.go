// Package logger builds the service-wide slog.Logger.
//
// Output is JSON on stdout. The platform's log shipper (set up by DevOps)
// ships stdout to Graylog over GELF — services never talk to Graylog directly.
//
// Every record logged with a context (slog.InfoContext etc.) automatically gets
// request_id, correlation_id, company/vertical/tenant IDs, actor and trace_id.
// Sensitive keys (password, otp, token, secret, …) are always redacted.
package logger

import (
	"context"
	"io"
	"log/slog"
	"os"
	"strings"

	"go.opentelemetry.io/otel/trace"

	"github.com/Sadiquzzaman/bus-2.0-go-boilerplate/platform/reqctx"
)

// Config controls logging. Env prefix is chosen by the embedding struct.
type Config struct {
	Level  string `env:"LEVEL" envDefault:"info"`  // debug|info|warn|error
	Format string `env:"FORMAT" envDefault:"json"` // json|text (text for local dev)
	// Extra keys to redact, comma separated, on top of the built-in list.
	RedactKeys []string `env:"REDACT_KEYS" envSeparator:","`
}

// Redacted is the replacement value for sensitive attributes.
const Redacted = "[REDACTED]"

var defaultRedact = []string{
	"password", "passwd", "secret", "token", "access_token", "refresh_token",
	"id_token", "authorization", "cookie", "set-cookie", "otp", "pin",
	"client_secret", "api_key", "apikey", "private_key", "card_number", "cvv",
}

// New builds a logger and installs it as slog's default.
func New(cfg Config, service, version, env string) *slog.Logger {
	return NewWithWriter(os.Stdout, cfg, service, version, env)
}

// NewWithWriter is New with a custom writer (tests).
func NewWithWriter(w io.Writer, cfg Config, service, version, env string) *slog.Logger {
	redact := make(map[string]struct{}, len(defaultRedact)+len(cfg.RedactKeys))
	for _, k := range append(defaultRedact, cfg.RedactKeys...) {
		if k = strings.TrimSpace(strings.ToLower(k)); k != "" {
			redact[k] = struct{}{}
		}
	}

	opts := &slog.HandlerOptions{
		Level:     parseLevel(cfg.Level),
		AddSource: false,
		ReplaceAttr: func(_ []string, a slog.Attr) slog.Attr {
			if _, ok := redact[strings.ToLower(a.Key)]; ok {
				return slog.String(a.Key, Redacted)
			}
			// Graylog/GELF friendly names.
			switch a.Key {
			case slog.MessageKey:
				a.Key = "message"
			case slog.TimeKey:
				a.Key = "timestamp"
			}
			return a
		},
	}

	var h slog.Handler
	if strings.EqualFold(cfg.Format, "text") {
		h = slog.NewTextHandler(w, opts)
	} else {
		h = slog.NewJSONHandler(w, opts)
	}

	l := slog.New(&contextHandler{Handler: h}).With(
		slog.String("service", service),
		slog.String("version", version),
		slog.String("env", env),
	)
	slog.SetDefault(l)
	return l
}

func parseLevel(s string) slog.Level {
	switch strings.ToLower(s) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

// contextHandler enriches records with values from the context.
type contextHandler struct{ slog.Handler }

func (h *contextHandler) Handle(ctx context.Context, r slog.Record) error {
	if ctx != nil {
		if v := reqctx.RequestID(ctx); v != "" {
			r.AddAttrs(slog.String("request_id", v))
		}
		if v := reqctx.CorrelationID(ctx); v != "" {
			r.AddAttrs(slog.String("correlation_id", v))
		}
		if s, ok := reqctx.ScopeFrom(ctx); ok {
			if s.CompanyID != "" {
				r.AddAttrs(slog.String("company_id", s.CompanyID))
			}
			if s.VerticalID != "" {
				r.AddAttrs(slog.String("vertical_id", s.VerticalID))
			}
			if s.TenantID != "" {
				r.AddAttrs(slog.String("tenant_id", s.TenantID))
			}
		}
		if a, ok := reqctx.ActorFrom(ctx); ok {
			r.AddAttrs(slog.String("actor_type", string(a.Type)), slog.String("actor_id", a.ID))
		}
		if sc := trace.SpanContextFromContext(ctx); sc.IsValid() {
			r.AddAttrs(slog.String("trace_id", sc.TraceID().String()), slog.String("span_id", sc.SpanID().String()))
		}
	}
	return h.Handler.Handle(ctx, r)
}

func (h *contextHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &contextHandler{Handler: h.Handler.WithAttrs(attrs)}
}

func (h *contextHandler) WithGroup(name string) slog.Handler {
	return &contextHandler{Handler: h.Handler.WithGroup(name)}
}

// Err is a shorthand attribute for errors.
func Err(err error) slog.Attr {
	if err == nil {
		return slog.Attr{}
	}
	return slog.String("error", err.Error())
}
