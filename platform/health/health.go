// Package health implements liveness and readiness.
//
//	/livez  — the process is alive. Never checks dependencies (a DB outage
//	          must not make Kubernetes restart every pod).
//	/readyz — the pod can take traffic: all registered checks pass and the
//	          service is not shutting down.
package health

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

// CheckFunc returns nil when the dependency is healthy.
type CheckFunc func(ctx context.Context) error

// Registry holds readiness checks.
type Registry struct {
	mu       sync.RWMutex
	checks   map[string]CheckFunc
	timeout  time.Duration
	draining atomic.Bool
}

// NewRegistry creates a registry; each check gets timeout to answer.
func NewRegistry(timeout time.Duration) *Registry {
	if timeout <= 0 {
		timeout = 2 * time.Second
	}
	return &Registry{checks: map[string]CheckFunc{}, timeout: timeout}
}

// Add registers a named readiness check.
func (r *Registry) Add(name string, fn CheckFunc) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.checks[name] = fn
}

// SetDraining marks the service as shutting down; readiness then fails.
func (r *Registry) SetDraining() { r.draining.Store(true) }

type result struct {
	Status string            `json:"status"`
	Checks map[string]string `json:"checks,omitempty"`
}

// Check runs every check concurrently.
func (r *Registry) Check(ctx context.Context) (bool, map[string]string) {
	r.mu.RLock()
	checks := make(map[string]CheckFunc, len(r.checks))
	for k, v := range r.checks {
		checks[k] = v
	}
	r.mu.RUnlock()

	out := make(map[string]string, len(checks))
	var mu sync.Mutex
	var wg sync.WaitGroup
	ok := true
	for name, fn := range checks {
		wg.Add(1)
		go func(name string, fn CheckFunc) {
			defer wg.Done()
			cctx, cancel := context.WithTimeout(ctx, r.timeout)
			defer cancel()
			err := fn(cctx)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				ok = false
				out[name] = err.Error()
			} else {
				out[name] = "ok"
			}
		}(name, fn)
	}
	wg.Wait()
	return ok, out
}

// LiveHandler always answers 200.
func (r *Registry) LiveHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, result{Status: "ok"})
	}
}

// ReadyHandler answers 200 when all checks pass, 503 otherwise.
func (r *Registry) ReadyHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		if r.draining.Load() {
			writeJSON(w, http.StatusServiceUnavailable, result{Status: "draining"})
			return
		}
		ok, checks := r.Check(req.Context())
		if !ok {
			writeJSON(w, http.StatusServiceUnavailable, result{Status: "unavailable", Checks: checks})
			return
		}
		writeJSON(w, http.StatusOK, result{Status: "ok", Checks: checks})
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
