// Package bootstrap wires example-mongo's dependencies and run modes.
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
	"github.com/Sadiquzzaman/bus-2.0-go-boilerplate/platform/kafka"
	pmongo "github.com/Sadiquzzaman/bus-2.0-go-boilerplate/platform/mongo"
	"github.com/Sadiquzzaman/bus-2.0-go-boilerplate/platform/outbox"
	"github.com/Sadiquzzaman/bus-2.0-go-boilerplate/platform/outbox/mongooutbox"

	"github.com/Sadiquzzaman/bus-2.0-go-boilerplate/services/example-mongo/internal/api"
	"github.com/Sadiquzzaman/bus-2.0-go-boilerplate/services/example-mongo/internal/config"
	"github.com/Sadiquzzaman/bus-2.0-go-boilerplate/services/example-mongo/internal/consumer"
	"github.com/Sadiquzzaman/bus-2.0-go-boilerplate/services/example-mongo/internal/events"
	"github.com/Sadiquzzaman/bus-2.0-go-boilerplate/services/example-mongo/internal/service"
	mongostore "github.com/Sadiquzzaman/bus-2.0-go-boilerplate/services/example-mongo/internal/store/mongo"
)

// Deps holds lazily created dependencies.
type Deps struct {
	a   *app.App
	cfg config.Config

	mu       sync.Mutex
	db       *pmongo.DB
	store    *mongostore.Store
	producer *kafka.Producer
}

// New creates an empty dependency set.
func New(a *app.App, cfg config.Config) *Deps { return &Deps{a: a, cfg: cfg} }

func (d *Deps) mongo(ctx context.Context) (*pmongo.DB, *mongostore.Store, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.db != nil {
		return d.db, d.store, nil
	}
	db, err := pmongo.Open(ctx, d.cfg.Mongo)
	if err != nil {
		return nil, nil, err
	}
	d.a.OnClose("mongo", db.Shutdown)
	d.a.Health.Add("mongo", db.Check)
	st := mongostore.New(db, d.cfg.App.Name)
	if err := st.EnsureIndexes(ctx); err != nil {
		return nil, nil, err
	}
	d.db, d.store = db, st
	return d.db, d.store, nil
}

// Store returns the service store.
func (d *Deps) Store(ctx context.Context) (service.Store, error) {
	_, s, err := d.mongo(ctx)
	return s, err
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

	srv, err := httpserver.New(d.cfg.HTTP, d.cfg.App.Name, d.a.Log)
	if err != nil {
		return err
	}
	v1 := srv.Engine.Group("/v1",
		auth.Authenticate(verifier),
		httpserver.Scope(),
	)
	items := service.NewItemService(store)
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
	db, _, err := d.mongo(ctx)
	if err != nil {
		return err
	}
	relay := outbox.NewRelay(
		mongooutbox.Store{DB: db.DB},
		mongooutbox.Leader{DB: db.DB, Name: d.cfg.App.Name + "-outbox-relay"},
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
