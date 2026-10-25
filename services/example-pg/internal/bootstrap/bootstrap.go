// Package bootstrap wires example-pg's dependencies and run modes.
//
// Dependencies are created lazily and at most once, so each run mode only
// connects to what it uses (the relay never opens the HTTP port, the API
// never joins a consumer group) and "all" can share them.
package bootstrap

import (
	"context"
	"sync"
	"time"

	"github.com/Sadiquzzaman/bus-2.0-go-boilerplate/platform/app"
	"github.com/Sadiquzzaman/bus-2.0-go-boilerplate/platform/auth"
	"github.com/Sadiquzzaman/bus-2.0-go-boilerplate/platform/httpserver"
	"github.com/Sadiquzzaman/bus-2.0-go-boilerplate/platform/idempotency"
	"github.com/Sadiquzzaman/bus-2.0-go-boilerplate/platform/kafka"
	"github.com/Sadiquzzaman/bus-2.0-go-boilerplate/platform/outbox"
	"github.com/Sadiquzzaman/bus-2.0-go-boilerplate/platform/outbox/pgoutbox"
	"github.com/Sadiquzzaman/bus-2.0-go-boilerplate/platform/postgres"
	"github.com/Sadiquzzaman/bus-2.0-go-boilerplate/platform/redis"

	"github.com/Sadiquzzaman/bus-2.0-go-boilerplate/services/example-pg/ent"
	"github.com/Sadiquzzaman/bus-2.0-go-boilerplate/services/example-pg/internal/api"
	"github.com/Sadiquzzaman/bus-2.0-go-boilerplate/services/example-pg/internal/config"
	"github.com/Sadiquzzaman/bus-2.0-go-boilerplate/services/example-pg/internal/consumer"
	"github.com/Sadiquzzaman/bus-2.0-go-boilerplate/services/example-pg/internal/events"
	"github.com/Sadiquzzaman/bus-2.0-go-boilerplate/services/example-pg/internal/service"
	pgstore "github.com/Sadiquzzaman/bus-2.0-go-boilerplate/services/example-pg/internal/store/postgres"
)

// Deps holds lazily created dependencies.
type Deps struct {
	a   *app.App
	cfg config.Config

	mu       sync.Mutex
	db       *postgres.DB
	client   *ent.Client
	store    *pgstore.Store
	rdb      *redis.Client
	producer *kafka.Producer
}

// New creates an empty dependency set.
func New(a *app.App, cfg config.Config) *Deps { return &Deps{a: a, cfg: cfg} }

func (d *Deps) postgres(ctx context.Context) (*postgres.DB, *pgstore.Store, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.db != nil {
		return d.db, d.store, nil
	}
	db, err := postgres.Open(ctx, d.cfg.Postgres)
	if err != nil {
		return nil, nil, err
	}
	d.a.OnClose("postgres", db.Shutdown)
	d.a.Health.Add("postgres", db.Check)
	d.db = db
	d.client = ent.NewClient(ent.Driver(db.Driver()))
	d.store = pgstore.New(d.client, d.cfg.App.Name)
	return d.db, d.store, nil
}

// Store returns the service store.
func (d *Deps) Store(ctx context.Context) (service.Store, error) {
	_, s, err := d.postgres(ctx)
	return s, err
}

// Redis returns the shared Redis client.
func (d *Deps) Redis(ctx context.Context) (*redis.Client, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.rdb != nil {
		return d.rdb, nil
	}
	rdb, err := redis.Open(ctx, d.cfg.Redis, d.cfg.App.Name)
	if err != nil {
		return nil, err
	}
	d.a.OnClose("redis", rdb.Shutdown)
	d.a.Health.Add("redis", rdb.Check)
	d.rdb = rdb
	return rdb, nil
}

// Producer returns the Kafka producer.
func (d *Deps) Producer() (*kafka.Producer, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.producer != nil {
		return d.producer, nil
	}
	p, err := kafka.NewProducer(d.cfg.Kafka, d.cfg.App.Name)
	if err != nil {
		return nil, err
	}
	d.a.OnClose("kafka-producer", p.Shutdown)
	d.a.Health.Add("kafka", p.Check)
	d.producer = p
	return p, nil
}

// RunAPI serves the public HTTP API.
func (d *Deps) RunAPI(ctx context.Context) error {
	verifier, err := auth.NewVerifier(ctx, d.cfg.Auth, d.cfg.App.Env)
	if err != nil {
		return err
	}
	authz, err := auth.NewAuthorizer(d.cfg.Auth.Authorizer, d.cfg.App.Env)
	if err != nil {
		return err
	}
	store, err := d.Store(ctx)
	if err != nil {
		return err
	}
	rdb, err := d.Redis(ctx)
	if err != nil {
		return err
	}

	srv, err := httpserver.New(d.cfg.HTTP, d.cfg.App.Name, d.a.Log)
	if err != nil {
		return err
	}
	v1 := srv.Engine.Group("/v1",
		auth.Authenticate(verifier),
		httpserver.Scope(),
		idempotency.Middleware(rdb, idempotency.Options{}),
	)
	items := service.NewItemService(store, redisCache{rdb})
	api.NewHandler(authz, items).Register(v1)

	return srv.Run(ctx, time.Duration(d.cfg.App.ShutdownTimeoutSeconds)*time.Second)
}

// RunRelay publishes outbox rows to Kafka. Run 2 replicas: one leads, the
// other waits as a hot standby.
func (d *Deps) RunRelay(ctx context.Context) error {
	p, err := d.Producer()
	if err != nil {
		return err
	}
	db, _, err := d.postgres(ctx)
	if err != nil {
		return err
	}
	relay := outbox.NewRelay(
		pgoutbox.Store{DB: db.SQL},
		pgoutbox.Leader{DB: db.SQL, Name: d.cfg.App.Name + "-outbox-relay"},
		p, d.cfg.Outbox, d.a.Log)
	return relay.Run(ctx)
}

var retryPolicy = kafka.RetryPolicy{Mode: kafka.RetryTopics}

func (d *Deps) consumerTopics() []string {
	if len(d.cfg.Consumer.Topics) > 0 {
		return d.cfg.Consumer.Topics
	}
	return events.OwnTopics
}

// RunConsumer runs the Kafka consumer group(s).
func (d *Deps) RunConsumer(ctx context.Context) error {
	store, err := d.Store(ctx)
	if err != nil {
		return err
	}
	h := consumer.New(store, d.a.Log)
	c, err := kafka.NewConsumer(d.cfg.Kafka, d.cfg.App.Name, kafka.ConsumerOptions{
		Group:   events.ConsumerGroup,
		Topics:  d.consumerTopics(),
		Handler: h.Router().Handler(),
		Retry:   retryPolicy,
	}, d.a.Log)
	if err != nil {
		return err
	}
	return c.Run(ctx)
}

// CreateTopics creates owned topics plus the consumer's retry/DLT topics.
// For local development only; in shared environments DevOps creates topics.
func (d *Deps) CreateTopics(ctx context.Context) error {
	topics := append(append([]string{}, events.OwnTopics...),
		kafka.ConsumerTopics(events.ConsumerGroup, d.consumerTopics(), retryPolicy)...)
	if err := kafka.EnsureTopics(ctx, d.cfg.Kafka, d.cfg.App.Name, topics...); err != nil {
		return err
	}
	d.a.Log.Info("topics ready", "topics", topics)
	return nil
}

// RunAll runs every mode in one process (local development only).
func (d *Deps) RunAll(ctx context.Context) error {
	return app.RunAll(ctx,
		d.RunAPI,
		d.RunRelay,
		d.RunConsumer,
	)
}

// redisCache adapts the Redis client to service.Cache.
type redisCache struct{ c *redis.Client }

func (r redisCache) Get(ctx context.Context, key string, dst any) (bool, error) {
	return r.c.GetJSON(ctx, r.c.Key(key), dst)
}

func (r redisCache) Set(ctx context.Context, key string, v any, ttl time.Duration) error {
	return r.c.SetJSON(ctx, r.c.Key(key), v, ttl)
}

func (r redisCache) Delete(ctx context.Context, keys ...string) error {
	full := make([]string, len(keys))
	for i, k := range keys {
		full[i] = r.c.Key(k)
	}
	return r.c.Del(ctx, full...).Err()
}
