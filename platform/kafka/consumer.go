package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"strconv"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/Sadiquzzaman/bus-2.0-go-boilerplate/platform/logger"
	"github.com/Sadiquzzaman/bus-2.0-go-boilerplate/platform/reqctx"
)

// Message is what a Handler receives.
type Message struct {
	Envelope         // decoded envelope (zero when ConsumerOptions.Raw)
	Topic     string // topic the record was read from (a retry topic on retries)
	Origin    string // original topic (same as Topic on first delivery)
	Partition int32
	Offset    int64
	Key       []byte
	Value     []byte // raw record value
	Attempt   int    // 0 on first delivery, n on the n-th retry
	Record    *kgo.Record
}

// Handler processes one message. Return nil to commit it, an error to retry
// it per the RetryPolicy, or Permanent(err) to send it to the dead-letter
// topic immediately. Handlers must be idempotent (see package inbox):
// messages can be redelivered after crashes and rebalances.
type Handler func(ctx context.Context, msg *Message) error

// Router dispatches by event_type. Unknown event types are skipped (and
// committed): readers ignore events they do not care about.
type Router struct {
	handlers map[string]Handler
	fallback Handler
}

// NewRouter creates an empty router.
func NewRouter() *Router { return &Router{handlers: map[string]Handler{}} }

// Handle registers h for eventType.
func (r *Router) Handle(eventType string, h Handler) *Router {
	r.handlers[eventType] = h
	return r
}

// Fallback handles event types with no registered handler.
func (r *Router) Fallback(h Handler) *Router { r.fallback = h; return r }

// Handler returns the dispatching Handler.
func (r *Router) Handler() Handler {
	return func(ctx context.Context, m *Message) error {
		if h, ok := r.handlers[m.EventType]; ok {
			return h(ctx, m)
		}
		if r.fallback != nil {
			return r.fallback(ctx, m)
		}
		return nil
	}
}

// RetryMode selects how failed messages are retried.
type RetryMode int

const (
	// RetryTopics re-publishes a failed message to a delay topic
	// (<topic>.<group>.retry.<n>) so the main partition keeps flowing.
	// After the last delay it goes to <topic>.<group>.dlt. Ordering per key
	// is NOT kept for retried messages. This is the default.
	RetryTopics RetryMode = iota
	// RetryInPlace retries the same message with backoff until it succeeds,
	// blocking its partition. Keeps strict per-key ordering. Use when the
	// handler owns retries itself (e.g. writes a retry row in its DB) and an
	// error only means infrastructure is down.
	RetryInPlace
)

// RetryPolicy configures failure handling.
type RetryPolicy struct {
	Mode RetryMode
	// Delays for RetryTopics; one retry topic per entry.
	// Default: 5s, 30s, 2m, 10m.
	Delays []time.Duration
	// Maximum backoff for RetryInPlace and for producing to retry/DLT topics.
	MaxBackoff time.Duration
}

func (p RetryPolicy) withDefaults() RetryPolicy {
	if p.Mode == RetryTopics && len(p.Delays) == 0 {
		p.Delays = []time.Duration{5 * time.Second, 30 * time.Second, 2 * time.Minute, 10 * time.Minute}
	}
	if p.MaxBackoff <= 0 {
		p.MaxBackoff = 30 * time.Second
	}
	return p
}

// ConsumerOptions configures one consumer group.
type ConsumerOptions struct {
	Group   string   // consumer group, e.g. "booking-seat-projector"
	Topics  []string // source topics
	Handler Handler
	Retry   RetryPolicy
	// Raw skips envelope decoding (for topics that do not carry envelopes).
	Raw bool
	// StartFromLatest makes a brand-new group skip existing messages.
	StartFromLatest bool
}

// Consumer runs a consumer group: a main lane on the source topics and, with
// RetryTopics, one lane per retry delay. Offsets are committed manually,
// only after the handler succeeded or the message was safely moved to a
// retry/dead-letter topic.
type Consumer struct {
	cfg     Config
	service string
	opts    ConsumerOptions
	log     *slog.Logger
	tracer  trace.Tracer
	dlq     *kgo.Client // producer for retry and dead-letter topics
}

var (
	consumedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "kafka_consumer_messages_total",
		Help: "Consumed messages by result (ok, retried, dead_lettered, skipped).",
	}, []string{"group", "topic", "result"})
	handleSeconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "kafka_consumer_handle_seconds",
		Help:    "Handler latency.",
		Buckets: prometheus.DefBuckets,
	}, []string{"group", "topic"})
)

// NewConsumer validates options and prepares the consumer.
func NewConsumer(cfg Config, service string, opts ConsumerOptions, log *slog.Logger) (*Consumer, error) {
	if opts.Group == "" || len(opts.Topics) == 0 || opts.Handler == nil {
		return nil, errors.New("kafka: consumer needs Group, Topics and Handler")
	}
	opts.Retry = opts.Retry.withDefaults()
	dlq, err := kgo.NewClient(append(cfg.baseOpts(service+"-"+opts.Group+"-dlq"),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProduceRequestTimeout(cfg.ProduceTimeout),
	)...)
	if err != nil {
		return nil, fmt.Errorf("kafka: consumer producer: %w", err)
	}
	return &Consumer{
		cfg: cfg, service: service, opts: opts,
		log:    log.With(slog.String("consumer_group", opts.Group)),
		tracer: otel.Tracer("platform/kafka"),
		dlq:    dlq,
	}, nil
}

// Topics lists every topic this consumer needs (sources, retries, DLT), for
// provisioning.
func (c *Consumer) Topics() []string {
	return ConsumerTopics(c.opts.Group, c.opts.Topics, c.opts.Retry)
}

// ConsumerTopics lists the source, retry and dead-letter topics a consumer
// group with this policy needs.
func ConsumerTopics(group string, topics []string, retry RetryPolicy) []string {
	retry = retry.withDefaults()
	out := append([]string{}, topics...)
	for _, t := range topics {
		if retry.Mode == RetryTopics {
			for i := range retry.Delays {
				out = append(out, RetryTopic(t, group, i+1))
			}
		}
		out = append(out, DLTopic(t, group))
	}
	return out
}

type lane struct {
	index  int // 0 = main, n = n-th retry
	group  string
	topics []string
	delay  time.Duration
}

func (c *Consumer) lanes() []lane {
	lanes := []lane{{index: 0, group: c.opts.Group, topics: c.opts.Topics}}
	if c.opts.Retry.Mode != RetryTopics {
		return lanes
	}
	for i, d := range c.opts.Retry.Delays {
		n := i + 1
		var ts []string
		for _, t := range c.opts.Topics {
			ts = append(ts, RetryTopic(t, c.opts.Group, n))
		}
		lanes = append(lanes, lane{index: n, group: c.opts.Group + ".retry." + strconv.Itoa(n), topics: ts, delay: d})
	}
	return lanes
}

// Run consumes until ctx is cancelled.
func (c *Consumer) Run(ctx context.Context) error {
	defer c.dlq.Close()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup
	errCh := make(chan error, 8)
	for _, l := range c.lanes() {
		wg.Add(1)
		go func(l lane) {
			defer wg.Done()
			if err := c.runLane(ctx, l); err != nil {
				errCh <- err
				cancel()
			}
		}(l)
	}
	wg.Wait()
	close(errCh)
	return <-errCh // nil if no lane failed
}

func (c *Consumer) runLane(ctx context.Context, l lane) error {
	reset := kgo.NewOffset().AtStart()
	if c.opts.StartFromLatest && l.index == 0 {
		reset = kgo.NewOffset().AtEnd()
	}
	cl, err := kgo.NewClient(append(c.cfg.baseOpts(c.service+"-"+l.group),
		kgo.ConsumerGroup(l.group),
		kgo.ConsumeTopics(l.topics...),
		kgo.DisableAutoCommit(),
		kgo.ConsumeResetOffset(reset),
		kgo.FetchMaxWait(time.Second),
	)...)
	if err != nil {
		return fmt.Errorf("kafka: consumer %s: %w", l.group, err)
	}
	defer cl.Close() // leaves the group cleanly

	c.log.Info("consumer started", slog.String("lane_group", l.group), slog.Any("topics", l.topics))
	for {
		fetches := cl.PollFetches(ctx)
		if fetches.IsClientClosed() || ctx.Err() != nil {
			return nil
		}
		fetches.EachError(func(t string, p int32, err error) {
			c.log.Warn("fetch error", slog.String("topic", t), slog.Int("partition", int(p)), logger.Err(err))
		})

		// Partitions are processed concurrently; records within a
		// partition sequentially, preserving per-key order.
		var wg sync.WaitGroup
		fetches.EachPartition(func(p kgo.FetchTopicPartition) {
			if len(p.Records) == 0 {
				return
			}
			wg.Add(1)
			go func(recs []*kgo.Record) {
				defer wg.Done()
				c.processPartition(ctx, cl, l, recs)
			}(p.Records)
		})
		wg.Wait()
	}
}

func (c *Consumer) processPartition(ctx context.Context, cl *kgo.Client, l lane, recs []*kgo.Record) {
	done := make([]*kgo.Record, 0, len(recs))
	defer func() {
		if len(done) == 0 {
			return
		}
		// Commit even if ctx was cancelled mid-batch.
		cctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := cl.CommitRecords(cctx, done...); err != nil {
			// Usually a rebalance: the records will be redelivered and
			// deduplicated by the handler's inbox.
			c.log.Warn("commit failed", logger.Err(err))
		}
	}()

	for _, rec := range recs {
		if l.delay > 0 && !c.waitUntilDue(ctx, rec) {
			return
		}
		if err := c.handleRecord(ctx, l, rec); err != nil {
			// Only returned when ctx ended before the record was settled.
			return
		}
		done = append(done, rec)
	}
}

// waitUntilDue sleeps until a retry record's due time. False if ctx ended.
func (c *Consumer) waitUntilDue(ctx context.Context, rec *kgo.Record) bool {
	due := rec.Timestamp
	if ms, err := strconv.ParseInt(Header(rec, HeaderRetryDueAt), 10, 64); err == nil {
		due = time.UnixMilli(ms)
	}
	d := time.Until(due)
	if d <= 0 {
		return true
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

// handleRecord settles one record: handled, retried, or dead-lettered.
// It returns an error only if ctx ended before the record was settled.
func (c *Consumer) handleRecord(ctx context.Context, l lane, rec *kgo.Record) error {
	origin := Header(rec, HeaderOriginalTopic)
	if origin == "" {
		origin = rec.Topic
	}
	attempt, _ := strconv.Atoi(Header(rec, HeaderRetryAttempt))

	msg := &Message{
		Topic: rec.Topic, Origin: origin, Partition: rec.Partition, Offset: rec.Offset,
		Key: rec.Key, Value: rec.Value, Attempt: attempt, Record: rec,
	}

	hctx := ExtractTrace(ctx, rec)
	hctx, span := c.tracer.Start(hctx, "consume "+origin, trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			attribute.String("messaging.system", "kafka"),
			attribute.String("messaging.destination.name", rec.Topic),
			attribute.String("messaging.consumer.group.name", c.opts.Group),
			attribute.Int64("messaging.kafka.offset", rec.Offset),
		))
	defer span.End()

	var err error
	if !c.opts.Raw {
		err = json.Unmarshal(rec.Value, &msg.Envelope)
		if err == nil {
			err = msg.Envelope.Validate()
		}
		if err != nil {
			err = Permanent(fmt.Errorf("invalid envelope: %w", err))
		}
	}
	if err == nil {
		hctx = msg.Envelope.Context(hctx)
		if id := Header(rec, HeaderCorrelationID); id != "" && reqctx.CorrelationID(hctx) == "" {
			hctx = reqctx.WithCorrelationID(hctx, id)
		}
		err = c.invoke(hctx, msg)
	}

	if err == nil {
		consumedTotal.WithLabelValues(c.opts.Group, origin, "ok").Inc()
		return nil
	}
	span.RecordError(err)
	if ctx.Err() != nil {
		// Shutting down: leave the record uncommitted; it is redelivered.
		return ctx.Err()
	}

	if !IsPermanent(err) && c.opts.Retry.Mode == RetryInPlace {
		return c.retryInPlace(hctx, msg, err)
	}

	log := c.log.With(slog.String("topic", rec.Topic), slog.Int64("offset", rec.Offset),
		slog.String("event_type", msg.EventType), slog.String("event_id", msg.EventID), slog.Int("attempt", attempt))

	next := attempt + 1
	if !IsPermanent(err) && c.opts.Retry.Mode == RetryTopics && next <= len(c.opts.Retry.Delays) {
		log.WarnContext(hctx, "handler failed, scheduling retry", logger.Err(err), slog.Int("next_attempt", next))
		out := c.copyRecord(rec, RetryTopic(origin, c.opts.Group, next), origin, err)
		setHeader(out, HeaderRetryAttempt, strconv.Itoa(next))
		setHeader(out, HeaderRetryDueAt, strconv.FormatInt(time.Now().Add(c.opts.Retry.Delays[next-1]).UnixMilli(), 10))
		if perr := c.produceUntilOK(ctx, out); perr != nil {
			return perr
		}
		consumedTotal.WithLabelValues(c.opts.Group, origin, "retried").Inc()
		return nil
	}

	log.ErrorContext(hctx, "handler failed, sending to dead-letter topic", logger.Err(err))
	out := c.copyRecord(rec, DLTopic(origin, c.opts.Group), origin, err)
	setHeader(out, HeaderFailedAt, time.Now().UTC().Format(time.RFC3339))
	if perr := c.produceUntilOK(ctx, out); perr != nil {
		return perr
	}
	consumedTotal.WithLabelValues(c.opts.Group, origin, "dead_lettered").Inc()
	return nil
}

func (c *Consumer) invoke(ctx context.Context, msg *Message) (err error) {
	start := time.Now()
	defer func() {
		handleSeconds.WithLabelValues(c.opts.Group, msg.Origin).Observe(time.Since(start).Seconds())
		if r := recover(); r != nil {
			err = fmt.Errorf("handler panic: %v", r)
		}
	}()
	return c.opts.Handler(ctx, msg)
}

func (c *Consumer) retryInPlace(ctx context.Context, msg *Message, first error) error {
	err := first
	for i := 1; ; i++ {
		c.log.WarnContext(ctx, "handler failed, retrying in place", logger.Err(err),
			slog.String("topic", msg.Topic), slog.Int64("offset", msg.Offset), slog.Int("try", i))
		if !sleep(ctx, backoff(i, c.opts.Retry.MaxBackoff)) {
			return ctx.Err()
		}
		err = c.invoke(ctx, msg)
		switch {
		case err == nil:
			consumedTotal.WithLabelValues(c.opts.Group, msg.Origin, "ok").Inc()
			return nil
		case IsPermanent(err):
			out := c.copyRecord(msg.Record, DLTopic(msg.Origin, c.opts.Group), msg.Origin, err)
			setHeader(out, HeaderFailedAt, time.Now().UTC().Format(time.RFC3339))
			if perr := c.produceUntilOK(ctx, out); perr != nil {
				return perr
			}
			consumedTotal.WithLabelValues(c.opts.Group, msg.Origin, "dead_lettered").Inc()
			return nil
		}
	}
}

func (c *Consumer) copyRecord(rec *kgo.Record, topic, origin string, cause error) *kgo.Record {
	out := &kgo.Record{Topic: topic, Key: rec.Key, Value: rec.Value}
	out.Headers = append(out.Headers, rec.Headers...)
	setHeader(out, HeaderOriginalTopic, origin)
	msg := cause.Error()
	if len(msg) > 1000 {
		msg = msg[:1000]
	}
	setHeader(out, HeaderError, msg)
	return out
}

// produceUntilOK keeps trying to move a record to a retry/DLT topic. If that
// is impossible, the consumer stops making progress rather than skipping.
func (c *Consumer) produceUntilOK(ctx context.Context, rec *kgo.Record) error {
	for i := 1; ; i++ {
		pctx, cancel := context.WithTimeout(ctx, c.cfg.ProduceTimeout+5*time.Second)
		err := c.dlq.ProduceSync(pctx, rec).FirstErr()
		cancel()
		if err == nil {
			return nil
		}
		c.log.Error("cannot produce to retry/dead-letter topic; partition is blocked",
			slog.String("topic", rec.Topic), logger.Err(err), slog.Int("try", i))
		if !sleep(ctx, backoff(i, c.opts.Retry.MaxBackoff)) {
			return ctx.Err()
		}
	}
}

func backoff(attempt int, max time.Duration) time.Duration {
	d := 200 * time.Millisecond << min(attempt, 10)
	if d > max {
		d = max
	}
	return d/2 + time.Duration(rand.Int64N(int64(d/2)+1))
}

func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
