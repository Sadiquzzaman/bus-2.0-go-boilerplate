package kafka

import (
	"context"
	"errors"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
)

// Header keys set on every record.
const (
	HeaderEventType     = "event_type"
	HeaderEventID       = "event_id"
	HeaderCorrelationID = "correlation_id"
	HeaderContentType   = "content-type"

	// Retry / dead-letter bookkeeping.
	HeaderRetryAttempt  = "x-retry-attempt"
	HeaderRetryDueAt    = "x-retry-due-at" // unix milliseconds
	HeaderOriginalTopic = "x-original-topic"
	HeaderError         = "x-error"
	HeaderFailedAt      = "x-failed-at"
)

// headerCarrier adapts record headers to OTel propagation.
type headerCarrier struct{ r *kgo.Record }

func (c headerCarrier) Get(key string) string {
	for _, h := range c.r.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

func (c headerCarrier) Set(key, val string) { setHeader(c.r, key, val) }

func (c headerCarrier) Keys() []string {
	out := make([]string, 0, len(c.r.Headers))
	for _, h := range c.r.Headers {
		out = append(out, h.Key)
	}
	return out
}

// InjectTrace writes the W3C trace context of ctx into r's headers.
func InjectTrace(ctx context.Context, r *kgo.Record) {
	otel.GetTextMapPropagator().Inject(ctx, headerCarrier{r})
}

// ExtractTrace returns ctx carrying the trace context found in r's headers.
func ExtractTrace(ctx context.Context, r *kgo.Record) context.Context {
	return otel.GetTextMapPropagator().Extract(ctx, headerCarrier{r})
}

// Header returns a header value.
func Header(r *kgo.Record, key string) string { return headerCarrier{r}.Get(key) }

func setHeader(r *kgo.Record, key, val string) {
	for i, h := range r.Headers {
		if h.Key == key {
			r.Headers[i].Value = []byte(val)
			return
		}
	}
	r.Headers = append(r.Headers, kgo.RecordHeader{Key: key, Value: []byte(val)})
}

// ---------------------------------------------------------------- errors

type permanentError struct{ err error }

func (p permanentError) Error() string { return "permanent: " + p.err.Error() }
func (p permanentError) Unwrap() error { return p.err }

// Permanent marks err as not worth retrying (bad data, failed validation,
// unknown reference). The message goes straight to the dead-letter topic.
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return permanentError{err}
}

// IsPermanent reports whether err was marked with Permanent.
func IsPermanent(err error) bool {
	var p permanentError
	return errors.As(err, &p)
}
