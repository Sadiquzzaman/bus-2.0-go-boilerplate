// Package config loads a service's configuration from environment variables
// (12-factor). Deployed environments set real env vars; locally
// an optional .env file in the working directory is read first.
//
// A service declares one struct that embeds the platform module configs:
//
//	type Config struct {
//	    App      config.App      `envPrefix:"APP_"`
//	    Log      logger.Config   `envPrefix:"LOG_"`
//	    HTTP     httpserver.Config `envPrefix:"HTTP_"`
//	    Postgres postgres.Config `envPrefix:"POSTGRES_"`
//	}
//
// and calls config.Load(&cfg). Startup fails fast on missing/invalid values.
package config

import (
	"errors"
	"fmt"
	"os"

	"github.com/caarlos0/env/v11"
	"github.com/joho/godotenv"
)

// Environment names.
const (
	EnvLocal      = "local"
	EnvTest       = "test"
	EnvStaging    = "staging"
	EnvProduction = "production"
)

// App holds settings every service has.
type App struct {
	Name    string `env:"NAME,required"`
	Env     string `env:"ENV" envDefault:"local"`
	Version string `env:"VERSION" envDefault:"dev"`
	// Seconds to keep serving after SIGTERM before shutting down, so
	// Kubernetes can remove the pod from Service endpoints first.
	ShutdownDelaySeconds   int `env:"SHUTDOWN_DELAY_SECONDS" envDefault:"5"`
	ShutdownTimeoutSeconds int `env:"SHUTDOWN_TIMEOUT_SECONDS" envDefault:"25"`
	// Address of the admin server (health, metrics) started in every run mode.
	AdminAddr string `env:"ADMIN_ADDR" envDefault:":8081"`
}

// IsProduction reports whether the service runs in production.
func (a App) IsProduction() bool { return a.Env == EnvProduction }

// IsLocal reports whether the service runs on a developer machine.
func (a App) IsLocal() bool { return a.Env == EnvLocal || a.Env == EnvTest }

// Validator is implemented by config structs that need cross-field checks.
type Validator interface{ Validate() error }

// Load reads .env (if present, never overriding real env vars), parses env
// vars into cfg and calls Validate on cfg and on any embedded Validator.
func Load(cfg any) error {
	if _, err := os.Stat(".env"); err == nil {
		if err := godotenv.Load(".env"); err != nil {
			return fmt.Errorf("config: read .env: %w", err)
		}
	}
	if err := env.Parse(cfg); err != nil {
		return fmt.Errorf("config: %w", err)
	}
	if v, ok := cfg.(Validator); ok {
		if err := v.Validate(); err != nil {
			return fmt.Errorf("config: %w", err)
		}
	}
	return nil
}

// MustLoad is Load that exits the process on error.
func MustLoad(cfg any) {
	if err := Load(cfg); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// Join runs several validations and joins their errors.
func Join(vs ...Validator) error {
	var errs []error
	for _, v := range vs {
		if v == nil {
			continue
		}
		if err := v.Validate(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}
