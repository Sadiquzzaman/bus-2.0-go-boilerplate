// Package app is the process lifecycle shared by every service.
//
// One binary, several run modes, chosen by the first CLI argument:
//
//	booking api        public HTTP API
//	booking relay      outbox → Kafka relay
//	booking consumer   Kafka consumers
//	booking worker     scheduled/background jobs
//	booking all        everything in one process (local development)
//
// Modes can be deployed and scaled independently from the same binary.
// Every mode also starts the admin server
// (/livez, /readyz, /metrics) used by probes and Prometheus.
//
// Shutdown on SIGTERM/SIGINT:
//  1. /readyz starts failing (pod is removed from Service endpoints)
//  2. wait APP_SHUTDOWN_DELAY_SECONDS
//  3. cancel the mode's context (HTTP server drains, consumers commit and stop)
//  4. run OnClose hooks in reverse order (Kafka, Redis, DB clients …)
package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"slices"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/Sadiquzzaman/bus-2.0-go-boilerplate/platform/config"
	"github.com/Sadiquzzaman/bus-2.0-go-boilerplate/platform/health"
	"github.com/Sadiquzzaman/bus-2.0-go-boilerplate/platform/logger"
	"github.com/Sadiquzzaman/bus-2.0-go-boilerplate/platform/observability"
)

// ModeFunc runs until ctx is cancelled or it fails.
type ModeFunc func(ctx context.Context) error

type mode struct {
	desc string
	fn   ModeFunc
	// admin: start the admin server for this mode (false for one-shot
	// commands like "topics").
	admin bool
}

type closer struct {
	name string
	fn   func(context.Context) error
}

// App owns logging, health, run modes and shutdown hooks.
type App struct {
	Cfg    config.App
	Log    *slog.Logger
	Health *health.Registry

	mu      sync.Mutex
	closers []closer
	modes   map[string]mode
}

// New initialises logging and tracing. Call it first in main().
func New(ctx context.Context, cfg config.App, logCfg logger.Config, obsCfg observability.Config) (*App, error) {
	log := logger.New(logCfg, cfg.Name, cfg.Version, cfg.Env)
	a := &App{
		Cfg:    cfg,
		Log:    log,
		Health: health.NewRegistry(2 * time.Second),
		modes:  map[string]mode{},
	}
	shutdownTracing, err := observability.Setup(ctx, obsCfg, cfg.Name, cfg.Version, cfg.Env)
	if err != nil {
		return nil, err
	}
	a.OnClose("tracing", shutdownTracing)
	return a, nil
}

// OnClose registers a shutdown hook. Hooks run in reverse registration
// order, so register clients right after creating them.
func (a *App) OnClose(name string, fn func(context.Context) error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.closers = append(a.closers, closer{name: name, fn: fn})
}

// Mode registers a long-running run mode.
func (a *App) Mode(name, desc string, fn ModeFunc) {
	a.modes[name] = mode{desc: desc, fn: fn, admin: true}
}

// Command registers a one-shot command (no admin server), e.g. "topics".
func (a *App) Command(name, desc string, fn ModeFunc) {
	a.modes[name] = mode{desc: desc, fn: fn, admin: false}
}

// Run executes the mode named by os.Args[1] (default "api") and returns the
// process exit code. Use: os.Exit(app.Run()).
func (a *App) Run() int {
	name := "api"
	if len(os.Args) > 1 {
		name = os.Args[1]
	}
	m, ok := a.modes[name]
	if !ok {
		a.usage(name)
		return 2
	}

	sigCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	modeCtx, cancelMode := context.WithCancel(context.Background())
	defer cancelMode()

	// Drain sequence on signal.
	go func() {
		select {
		case <-sigCtx.Done():
		case <-modeCtx.Done():
			return
		}
		a.Log.Info("shutdown signal received", slog.String("mode", name))
		a.Health.SetDraining()
		if m.admin && a.Cfg.ShutdownDelaySeconds > 0 {
			time.Sleep(time.Duration(a.Cfg.ShutdownDelaySeconds) * time.Second)
		}
		cancelMode()
	}()

	var adminSrv *http.Server
	if m.admin {
		adminSrv = a.startAdmin()
	}

	a.Log.Info("starting", slog.String("mode", name), slog.String("version", a.Cfg.Version), slog.String("env", a.Cfg.Env))
	runErr := m.fn(modeCtx)
	cancelMode()
	if runErr != nil && !errors.Is(runErr, context.Canceled) {
		a.Log.Error("mode failed", slog.String("mode", name), logger.Err(runErr))
	}

	timeout := time.Duration(a.Cfg.ShutdownTimeoutSeconds) * time.Second
	shutdownCtx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if adminSrv != nil {
		_ = adminSrv.Shutdown(shutdownCtx)
	}
	a.close(shutdownCtx)

	if runErr != nil && !errors.Is(runErr, context.Canceled) {
		return 1
	}
	a.Log.Info("stopped", slog.String("mode", name))
	return 0
}

func (a *App) close(ctx context.Context) {
	a.mu.Lock()
	cs := slices.Clone(a.closers)
	a.mu.Unlock()
	for i := len(cs) - 1; i >= 0; i-- {
		if err := cs[i].fn(ctx); err != nil {
			a.Log.Warn("close failed", slog.String("component", cs[i].name), logger.Err(err))
		}
	}
}

func (a *App) startAdmin() *http.Server {
	mux := http.NewServeMux()
	mux.Handle("/livez", a.Health.LiveHandler())
	mux.Handle("/readyz", a.Health.ReadyHandler())
	mux.Handle("/metrics", observability.MetricsHandler())
	srv := &http.Server{Addr: a.Cfg.AdminAddr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			a.Log.Error("admin server failed", logger.Err(err))
		}
	}()
	a.Log.Info("admin server listening", slog.String("addr", a.Cfg.AdminAddr))
	return srv
}

func (a *App) usage(got string) {
	names := make([]string, 0, len(a.modes))
	for n := range a.modes {
		names = append(names, n)
	}
	sort.Strings(names)
	var b strings.Builder
	fmt.Fprintf(&b, "unknown mode %q\n\nusage: %s <mode>\n\nmodes:\n", got, a.Cfg.Name)
	for _, n := range names {
		fmt.Fprintf(&b, "  %-10s %s\n", n, a.modes[n].desc)
	}
	fmt.Fprint(os.Stderr, b.String())
}

// RunAll runs fns concurrently. The first error cancels the rest; it returns
// once all have stopped. Use it for the "all" mode or for modes that run
// several components (e.g. two consumers).
func RunAll(ctx context.Context, fns ...ModeFunc) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var (
		wg       sync.WaitGroup
		once     sync.Once
		firstErr error
	)
	for _, fn := range fns {
		wg.Add(1)
		go func(fn ModeFunc) {
			defer wg.Done()
			if err := fn(ctx); err != nil && !errors.Is(err, context.Canceled) {
				once.Do(func() { firstErr = err; cancel() })
			}
		}(fn)
	}
	wg.Wait()
	return firstErr
}
