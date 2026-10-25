// Package redis wraps go-redis v9 with the helpers services need:
// namespaced keys, JSON cache, distributed locks and rate limiting.
//
// Redis holds short-lived data only (caches, locks, counters, idempotency
// results). Nothing in Redis may be the only copy of business data.
package redis

import (
	"context"
	"crypto/tls"
	"fmt"
	"strings"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

// Config supports standalone, Sentinel (MasterName set) and Cluster
// (several Addrs and ClusterMode).
type Config struct {
	Addrs        []string      `env:"ADDRS" envSeparator:"," envDefault:"localhost:6379"`
	MasterName   string        `env:"MASTER_NAME"` // Sentinel
	ClusterMode  bool          `env:"CLUSTER_MODE" envDefault:"false"`
	Username     string        `env:"USERNAME"`
	Password     string        `env:"PASSWORD"`
	DB           int           `env:"DB" envDefault:"0"`
	TLS          bool          `env:"TLS" envDefault:"false"`
	PoolSize     int           `env:"POOL_SIZE" envDefault:"0"` // 0 = go-redis default
	DialTimeout  time.Duration `env:"DIAL_TIMEOUT" envDefault:"5s"`
	ReadTimeout  time.Duration `env:"READ_TIMEOUT" envDefault:"2s"`
	WriteTimeout time.Duration `env:"WRITE_TIMEOUT" envDefault:"2s"`
}

// Client is a namespaced Redis client. All helper keys are prefixed with
// "<service>:" so services sharing a Redis never collide.
type Client struct {
	goredis.UniversalClient
	prefix string
}

// Open connects and pings.
func Open(ctx context.Context, cfg Config, service string) (*Client, error) {
	opts := &goredis.UniversalOptions{
		Addrs:        cfg.Addrs,
		MasterName:   cfg.MasterName,
		Username:     cfg.Username,
		Password:     cfg.Password,
		DB:           cfg.DB,
		PoolSize:     cfg.PoolSize,
		DialTimeout:  cfg.DialTimeout,
		ReadTimeout:  cfg.ReadTimeout,
		WriteTimeout: cfg.WriteTimeout,
	}
	if cfg.TLS {
		opts.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	}
	var c goredis.UniversalClient
	if cfg.ClusterMode {
		c = goredis.NewClusterClient(opts.Cluster())
	} else {
		c = goredis.NewUniversalClient(opts)
	}
	pctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := c.Ping(pctx).Err(); err != nil {
		_ = c.Close()
		return nil, fmt.Errorf("redis: ping: %w", err)
	}
	return &Client{UniversalClient: c, prefix: service}, nil
}

// Key builds "<service>:<parts…>". Include the company ID in keys for
// company-scoped data: c.Key("trip", companyID, tripID).
func (c *Client) Key(parts ...string) string {
	return c.prefix + ":" + strings.Join(parts, ":")
}

// Check is a health.CheckFunc.
func (c *Client) Check(ctx context.Context) error { return c.Ping(ctx).Err() }

// Shutdown closes the client (signature fits app.OnClose).
func (c *Client) Shutdown(context.Context) error { return c.Close() }

// IsNil reports a missing key.
func IsNil(err error) bool { return err == goredis.Nil }
