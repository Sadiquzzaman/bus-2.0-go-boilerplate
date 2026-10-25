package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/twmb/franz-go/pkg/kgo"
)

var producedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "kafka_producer_messages_total",
	Help: "Messages produced, by topic and result.",
}, []string{"topic", "result"})

// Producer publishes records synchronously with acks=all and idempotence
// (franz-go defaults), so a nil error means the record is durably stored.
//
// Business events should normally go through the outbox, not straight
// through Producer. Publish directly only when losing the event on a crash
// is acceptable or the event must not be stored (e.g. OTP messages).
type Producer struct {
	cl      *kgo.Client
	service string
	cfg     Config
}

// NewProducer creates a producer for service.
func NewProducer(cfg Config, service string) (*Producer, error) {
	opts := append(cfg.baseOpts(service),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProduceRequestTimeout(cfg.ProduceTimeout),
		kgo.RecordRetries(10),
	)
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("kafka: producer: %w", err)
	}
	return &Producer{cl: cl, service: service, cfg: cfg}, nil
}

// Publish sends one envelope to topic, keyed by its aggregate ID.
func (p *Producer) Publish(ctx context.Context, topic string, env Envelope) error {
	rec, err := p.Record(ctx, topic, env)
	if err != nil {
		return err
	}
	return p.PublishRecords(ctx, rec)
}

// Record builds the kgo.Record for env (headers, trace context, key).
func (p *Producer) Record(ctx context.Context, topic string, env Envelope) (*kgo.Record, error) {
	return BuildRecord(ctx, p.service, topic, env)
}

// BuildRecord marshals env into a record for topic. Producer is set to
// service if empty.
func BuildRecord(ctx context.Context, service, topic string, env Envelope) (*kgo.Record, error) {
	if env.Producer == "" {
		env.Producer = service
	}
	if err := env.Validate(); err != nil {
		return nil, err
	}
	b, err := json.Marshal(env)
	if err != nil {
		return nil, fmt.Errorf("kafka: marshal envelope: %w", err)
	}
	r := &kgo.Record{Topic: topic, Key: []byte(env.Key()), Value: b}
	setHeader(r, HeaderContentType, "application/json")
	setHeader(r, HeaderEventType, env.EventType)
	setHeader(r, HeaderEventID, env.EventID)
	if env.CorrelationID != "" {
		setHeader(r, HeaderCorrelationID, env.CorrelationID)
	}
	InjectTrace(ctx, r)
	return r, nil
}

// PublishRecords sends records and waits for all acknowledgements.
func (p *Producer) PublishRecords(ctx context.Context, recs ...*kgo.Record) error {
	if len(recs) == 0 {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, p.cfg.ProduceTimeout+5*time.Second)
	defer cancel()
	results := p.cl.ProduceSync(ctx, recs...)
	for _, r := range results {
		res := "ok"
		if r.Err != nil {
			res = "error"
		}
		producedTotal.WithLabelValues(r.Record.Topic, res).Inc()
	}
	if err := results.FirstErr(); err != nil {
		return fmt.Errorf("kafka: produce: %w", err)
	}
	return nil
}

// ProduceEach sends records and returns one error (or nil) per record, in
// the same order as recs. Used by the outbox relay to mark rows precisely.
func (p *Producer) ProduceEach(ctx context.Context, recs ...*kgo.Record) []error {
	ctx, cancel := context.WithTimeout(ctx, p.cfg.ProduceTimeout+5*time.Second)
	defer cancel()
	byRec := make(map[*kgo.Record]error, len(recs))
	for _, r := range p.cl.ProduceSync(ctx, recs...) {
		byRec[r.Record] = r.Err
		res := "ok"
		if r.Err != nil {
			res = "error"
		}
		producedTotal.WithLabelValues(r.Record.Topic, res).Inc()
	}
	out := make([]error, len(recs))
	for i, r := range recs {
		out[i] = byRec[r]
	}
	return out
}

// Check is a health.CheckFunc.
func (p *Producer) Check(ctx context.Context) error { return p.cl.Ping(ctx) }

// Shutdown flushes buffered records and closes the client.
func (p *Producer) Shutdown(ctx context.Context) error {
	err := p.cl.Flush(ctx)
	p.cl.Close()
	return err
}
