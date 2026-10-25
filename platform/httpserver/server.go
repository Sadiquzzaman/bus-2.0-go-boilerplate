// Package httpserver builds the public Gin server every service uses, with
// the standard middleware chain:
//
//	recovery → request ID → tracing → metrics → access log → timeout → CORS
//
// followed by service routes. Health and metrics are NOT on this server; they
// live on the admin server (see platform/app) so they are never exposed
// through the ingress.
package httpserver

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin"
)

// Config for the public HTTP server.
type Config struct {
	Addr               string        `env:"ADDR" envDefault:":8080"`
	ReadTimeout        time.Duration `env:"READ_TIMEOUT" envDefault:"15s"`
	WriteTimeout       time.Duration `env:"WRITE_TIMEOUT" envDefault:"30s"`
	IdleTimeout        time.Duration `env:"IDLE_TIMEOUT" envDefault:"120s"`
	RequestTimeout     time.Duration `env:"REQUEST_TIMEOUT" envDefault:"20s"`
	MaxBodyBytes       int64         `env:"MAX_BODY_BYTES" envDefault:"4194304"` // 4 MiB
	CORSAllowedOrigins []string      `env:"CORS_ALLOWED_ORIGINS" envSeparator:","`
	TrustedProxies     []string      `env:"TRUSTED_PROXIES" envSeparator:"," envDefault:"10.0.0.0/8,172.16.0.0/12,192.168.0.0/16"`
	AccessLogSkipPaths []string      `env:"ACCESS_LOG_SKIP_PATHS" envSeparator:","`
	DebugMode          bool          `env:"GIN_DEBUG" envDefault:"false"`
}

// Server wraps a Gin engine and its http.Server.
type Server struct {
	Engine *gin.Engine
	srv    *http.Server
	log    *slog.Logger
}

// New creates the engine with the standard middleware. Add routes to
// s.Engine (or groups of it) before calling Start.
func New(cfg Config, service string, log *slog.Logger) (*Server, error) {
	if cfg.DebugMode {
		gin.SetMode(gin.DebugMode)
	} else {
		gin.SetMode(gin.ReleaseMode)
	}
	e := gin.New()
	e.HandleMethodNotAllowed = true
	if err := e.SetTrustedProxies(cfg.TrustedProxies); err != nil {
		return nil, fmt.Errorf("httpserver: trusted proxies: %w", err)
	}

	e.Use(
		Recovery(log),
		RequestID(),
		otelgin.Middleware(service),
		Metrics(service),
		AccessLog(log, cfg.AccessLogSkipPaths...),
		Timeout(cfg.RequestTimeout),
		BodyLimit(cfg.MaxBodyBytes),
		CORS(cfg.CORSAllowedOrigins),
	)
	e.NoRoute(func(c *gin.Context) { Fail(c, errNotFoundRoute) })
	e.NoMethod(func(c *gin.Context) { Fail(c, errMethodNotAllowed) })

	return &Server{
		Engine: e,
		log:    log,
		srv: &http.Server{
			Addr:              cfg.Addr,
			Handler:           e,
			ReadTimeout:       cfg.ReadTimeout,
			ReadHeaderTimeout: 5 * time.Second,
			WriteTimeout:      cfg.WriteTimeout,
			IdleTimeout:       cfg.IdleTimeout,
		},
	}, nil
}

// Run serves until ctx is cancelled, then shuts down gracefully within
// shutdownTimeout.
func (s *Server) Run(ctx context.Context, shutdownTimeout time.Duration) error {
	ln, err := net.Listen("tcp", s.srv.Addr)
	if err != nil {
		return fmt.Errorf("httpserver: listen %s: %w", s.srv.Addr, err)
	}
	s.log.Info("http server listening", slog.String("addr", s.srv.Addr))

	errCh := make(chan error, 1)
	go func() { errCh <- s.srv.Serve(ln) }()

	select {
	case err := <-errCh:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
	}

	sctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	s.log.Info("http server shutting down")
	if err := s.srv.Shutdown(sctx); err != nil {
		return fmt.Errorf("httpserver: shutdown: %w", err)
	}
	return nil
}
