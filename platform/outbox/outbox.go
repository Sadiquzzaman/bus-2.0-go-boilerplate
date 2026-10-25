// Package outbox implements the transactional outbox.
//
// Write side (inside the business transaction):
//
//	w := pgoutbox.Writer{Service: "booking"}
//	tx, _ := client.Tx(ctx)                       // ent, built with --feature sql/execquery
//	trip, _ := tx.Trip.Create()....Save(ctx)
//	env, _ := kafka.NewEnvelope(ctx, kafka.Event{Type: "trip.created", AggregateID: trip.ID.String(), Payload: trip})
//	_ = w.Add(ctx, tx, "booking.trip.v1", env)    // same transaction
//	tx.Commit()
//
// (Mongo services use mongooutbox.Writer inside mongo.DB.WithTx.)
//
// Relay side (the `relay` run mode): one elected leader reads unpublished rows
// in insertion order, publishes them to Kafka (acks=all) and marks them
// published. Other replicas wait as hot standbys. Published rows are deleted
// after a retention period.
//
// Delivery is at-least-once: a crash between publish and mark re-sends the
// row, so consumers must be idempotent (package inbox).
package outbox

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Sadiquzzaman/bus-2.0-go-boilerplate/platform/kafka"
	"github.com/Sadiquzzaman/bus-2.0-go-boilerplate/platform/logger"
)

// Row is one stored message.
type Row struct {
	ID       string
	Topic    string
	Key      string
	Payload  []byte
	Headers  map[string]string
	Attempts int
}

// Store is the relay's view of the outbox table/collection.
type Store interface {
	Fetch(ctx context.Context, limit int) ([]Row, error)
	MarkPublished(ctx context.Context, ids []string) error
	MarkFailed(ctx context.Context, id string, cause error) error
	Pending(ctx context.Context) (int64, error)
	Cleanup(ctx context.Context, publishedBefore time.Time) (int64, error)
}

// Leader elects a single relay. Acquire blocks until this replica leads or
// ctx ends; the returned context is cancelled if leadership is lost.
type Leader interface {
	Acquire(ctx context.Context) (lead context.Context, release func(), err error)
}

// RelayConfig tunes the relay. Env prefix chosen by the embedding struct.
type RelayConfig struct {
	BatchSize       int           `env:"BATCH_SIZE" envDefault:"200"`
	PollInterval    time.Duration `env:"POLL_INTERVAL" envDefault:"300ms"`
	Retention       time.Duration `env:"RETENTION" envDefault:"168h"`
	CleanupInterval time.Duration `env:"CLEANUP_INTERVAL" envDefault:"1h"`
}

var (
	pendingGauge = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "outbox_pending_messages", Help: "Unpublished outbox rows.",
	})
	publishedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "outbox_published_total", Help: "Outbox rows published.",
	})
	failedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "outbox_publish_failures_total", Help: "Outbox publish failures.",
	})
)

// Relay moves outbox rows to Kafka.
type Relay struct {
	store  Store
	leader Leader
	pub    *kafka.Producer
	cfg    RelayConfig
	log    *slog.Logger
}

// NewRelay builds a relay.
func NewRelay(store Store, leader Leader, pub *kafka.Producer, cfg RelayConfig, log *slog.Logger) *Relay {
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 200
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 300 * time.Millisecond
	}
	return &Relay{store: store, leader: leader, pub: pub, cfg: cfg, log: log.With(slog.String("component", "outbox-relay"))}
}

// Run blocks until ctx ends, leading whenever it can.
func (r *Relay) Run(ctx context.Context) error {
	for ctx.Err() == nil {
		r.log.Info("waiting for leadership")
		lead, release, err := r.leader.Acquire(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			r.log.Warn("leader election failed", logger.Err(err))
			Sleep(ctx, 5*time.Second)
			continue
		}
		r.log.Info("acquired leadership, relaying")
		r.lead(lead)
		release()
		r.log.Info("leadership released")
	}
	return nil
}

func (r *Relay) lead(ctx context.Context) {
	poll := time.NewTicker(r.cfg.PollInterval)
	defer poll.Stop()
	cleanup := time.NewTicker(r.cfg.CleanupInterval)
	defer cleanup.Stop()
	metrics := time.NewTicker(15 * time.Second)
	defer metrics.Stop()

	failures := 0
	for {
		// Drain while there is work; otherwise wait for the next tick.
		n, err := r.publishBatch(ctx)
		switch {
		case err != nil:
			failures++
			failedTotal.Inc()
			r.log.Error("publish batch failed", logger.Err(err), slog.Int("consecutive_failures", failures))
			Sleep(ctx, min(time.Duration(failures)*time.Second, 30*time.Second))
		case n == r.cfg.BatchSize:
			failures = 0
			if ctx.Err() != nil {
				return
			}
			continue
		default:
			failures = 0
		}
		select {
		case <-ctx.Done():
			return
		case <-poll.C:
		case <-metrics.C:
			if p, err := r.store.Pending(ctx); err == nil {
				pendingGauge.Set(float64(p))
			}
		case <-cleanup.C:
			if r.cfg.Retention > 0 {
				if n, err := r.store.Cleanup(ctx, time.Now().Add(-r.cfg.Retention)); err != nil {
					r.log.Warn("outbox cleanup failed", logger.Err(err))
				} else if n > 0 {
					r.log.Info("outbox cleanup", slog.Int64("deleted", n))
				}
			}
		}
	}
}

// publishBatch publishes one batch. Rows before the first failure are marked
// published; the failing row and everything after it stay pending and are
// sent again (consumers deduplicate by event_id), so no row is ever skipped.
func (r *Relay) publishBatch(ctx context.Context) (int, error) {
	rows, err := r.store.Fetch(ctx, r.cfg.BatchSize)
	if err != nil || len(rows) == 0 {
		return 0, err
	}
	recs := make([]*kgo.Record, len(rows))
	for i, row := range rows {
		rec := &kgo.Record{Topic: row.Topic, Key: []byte(row.Key), Value: row.Payload}
		for k, v := range row.Headers {
			rec.Headers = append(rec.Headers, kgo.RecordHeader{Key: k, Value: []byte(v)})
		}
		recs[i] = rec
	}

	results := r.pub.ProduceEach(ctx, recs...)
	var ok []string
	for i, res := range results {
		if res != nil {
			_ = r.store.MarkFailed(ctx, rows[i].ID, res)
			if len(ok) > 0 {
				if err := r.store.MarkPublished(ctx, ok); err != nil {
					return 0, err
				}
				publishedTotal.Add(float64(len(ok)))
			}
			return len(ok), res
		}
		ok = append(ok, rows[i].ID)
	}
	if err := r.store.MarkPublished(ctx, ok); err != nil {
		return 0, err
	}
	publishedTotal.Add(float64(len(ok)))
	return len(ok), nil
}

// Sleep waits for d or until ctx ends.
func Sleep(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}

// HeadersJSON encodes row headers for storage.
func HeadersJSON(h map[string]string) ([]byte, error) {
	if h == nil {
		h = map[string]string{}
	}
	return json.Marshal(h)
}

// NewRow converts an envelope into a row via kafka.BuildRecord, so
// stored headers (event type, trace context) match a direct publish.
func NewRow(ctx context.Context, service, topic string, env kafka.Envelope) (Row, error) {
	rec, err := kafka.BuildRecord(ctx, service, topic, env)
	if err != nil {
		return Row{}, err
	}
	h := make(map[string]string, len(rec.Headers))
	for _, kv := range rec.Headers {
		h[kv.Key] = string(kv.Value)
	}
	return Row{ID: env.EventID, Topic: topic, Key: string(rec.Key), Payload: rec.Value, Headers: h}, nil
}
