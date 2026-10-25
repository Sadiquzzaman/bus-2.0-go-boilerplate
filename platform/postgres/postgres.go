// Package postgres opens PostgreSQL through pgx and exposes it to ent.
//
//	db, err := postgres.Open(ctx, cfg)
//	client := ent.NewClient(ent.Driver(db.Driver()))
//
// The same *sql.DB is used by the outbox and inbox (raw SQL that joins the
// service's ent transaction, see package outbox).
package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"entgo.io/ent/dialect"
	entsql "entgo.io/ent/dialect/sql"
	"github.com/XSAM/otelsql"
	_ "github.com/jackc/pgx/v5/stdlib" // registers the "pgx" driver
	"github.com/prometheus/client_golang/prometheus"
)

// Config for the connection pool.
type Config struct {
	DSN             string        `env:"DSN,required"` // postgres://user:pass@host:5432/db?sslmode=disable
	MaxOpenConns    int           `env:"MAX_OPEN_CONNS" envDefault:"20"`
	MaxIdleConns    int           `env:"MAX_IDLE_CONNS" envDefault:"10"`
	ConnMaxLifetime time.Duration `env:"CONN_MAX_LIFETIME" envDefault:"30m"`
	ConnMaxIdleTime time.Duration `env:"CONN_MAX_IDLE_TIME" envDefault:"5m"`
	// Log every SQL statement (debug only).
	Debug bool `env:"DEBUG" envDefault:"false"`
}

// DB is an open PostgreSQL pool.
type DB struct {
	SQL   *sql.DB
	debug bool
}

// Open connects and pings. The pool is instrumented for tracing and exports
// pool stats as Prometheus metrics.
func Open(ctx context.Context, cfg Config) (*DB, error) {
	db, err := otelsql.Open("pgx", cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("postgres: open: %w", err)
	}
	db.SetMaxOpenConns(cfg.MaxOpenConns)
	db.SetMaxIdleConns(cfg.MaxIdleConns)
	db.SetConnMaxLifetime(cfg.ConnMaxLifetime)
	db.SetConnMaxIdleTime(cfg.ConnMaxIdleTime)

	pctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := db.PingContext(pctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("postgres: ping: %w", err)
	}
	_ = prometheus.Register(newPoolCollector(db))
	return &DB{SQL: db, debug: cfg.Debug}, nil
}

// Driver returns an ent driver over the pool.
func (d *DB) Driver() dialect.Driver {
	drv := entsql.OpenDB(dialect.Postgres, d.SQL)
	if d.debug {
		return dialect.Debug(drv)
	}
	return drv
}

// Check is a health.CheckFunc.
func (d *DB) Check(ctx context.Context) error { return d.SQL.PingContext(ctx) }

// Shutdown closes the pool (signature fits app.OnClose).
func (d *DB) Shutdown(context.Context) error { return d.SQL.Close() }

// poolCollector exports database/sql pool stats.
type poolCollector struct {
	db                          *sql.DB
	open, inUse, idle, waitTime *prometheus.Desc
}

func newPoolCollector(db *sql.DB) *poolCollector {
	return &poolCollector{
		db:       db,
		open:     prometheus.NewDesc("db_pool_open_connections", "Open connections.", nil, nil),
		inUse:    prometheus.NewDesc("db_pool_in_use_connections", "Connections in use.", nil, nil),
		idle:     prometheus.NewDesc("db_pool_idle_connections", "Idle connections.", nil, nil),
		waitTime: prometheus.NewDesc("db_pool_wait_seconds_total", "Total time waited for a connection.", nil, nil),
	}
}

func (c *poolCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.open
	ch <- c.inUse
	ch <- c.idle
	ch <- c.waitTime
}

func (c *poolCollector) Collect(ch chan<- prometheus.Metric) {
	s := c.db.Stats()
	ch <- prometheus.MustNewConstMetric(c.open, prometheus.GaugeValue, float64(s.OpenConnections))
	ch <- prometheus.MustNewConstMetric(c.inUse, prometheus.GaugeValue, float64(s.InUse))
	ch <- prometheus.MustNewConstMetric(c.idle, prometheus.GaugeValue, float64(s.Idle))
	ch <- prometheus.MustNewConstMetric(c.waitTime, prometheus.CounterValue, s.WaitDuration.Seconds())
}
