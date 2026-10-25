package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/Sadiquzzaman/bus-2.0-go-boilerplate/platform/reqctx"
)

// Envelope is the JSON value of every event on every topic.
// The JSON Schema lives in contracts/envelope.schema.json.
//
// Rules: only add optional fields; a breaking change is a new EventVersion
// published alongside the old one until all readers moved. Readers ignore
// unknown fields. Never put passwords, tokens or secrets in an event.
type Envelope struct {
	EventID       string          `json:"event_id"`
	EventType     string          `json:"event_type"`
	EventVersion  int             `json:"event_version"`
	OccurredAt    time.Time       `json:"occurred_at"`
	Producer      string          `json:"producer"`
	AggregateType string          `json:"aggregate_type,omitempty"`
	AggregateID   string          `json:"aggregate_id,omitempty"`
	CompanyID     string          `json:"company_id,omitempty"`
	VerticalID    string          `json:"vertical_id,omitempty"`
	TenantID      string          `json:"tenant_id,omitempty"`
	CorrelationID string          `json:"correlation_id,omitempty"`
	Actor         *reqctx.Actor   `json:"actor,omitempty"`
	Payload       json.RawMessage `json:"payload"`
}

// Event is what services fill in; NewEnvelope adds the rest.
type Event struct {
	Type          string // "seat.reserved"
	Version       int    // payload schema version, starts at 1
	AggregateType string // "seat"
	AggregateID   string // used as the Kafka message key
	Payload       any
}

// NewEnvelope builds an envelope, taking scope, actor and correlation ID from
// ctx. Producer is filled in when published (Producer / outbox writer).
func NewEnvelope(ctx context.Context, e Event) (Envelope, error) {
	if e.Type == "" {
		return Envelope{}, errors.New("kafka: event type is required")
	}
	if e.Version < 1 {
		e.Version = 1
	}
	payload, err := json.Marshal(e.Payload)
	if err != nil {
		return Envelope{}, fmt.Errorf("kafka: marshal payload: %w", err)
	}
	env := Envelope{
		EventID:       uuid.Must(uuid.NewV7()).String(),
		EventType:     e.Type,
		EventVersion:  e.Version,
		OccurredAt:    time.Now().UTC(),
		AggregateType: e.AggregateType,
		AggregateID:   e.AggregateID,
		CorrelationID: reqctx.CorrelationID(ctx),
		Payload:       payload,
	}
	if s, ok := reqctx.ScopeFrom(ctx); ok {
		env.CompanyID, env.VerticalID, env.TenantID = s.CompanyID, s.VerticalID, s.TenantID
	}
	if a, ok := reqctx.ActorFrom(ctx); ok {
		env.Actor = &a
	}
	return env, nil
}

// Key returns the message key: the aggregate ID, or the event ID.
func (e Envelope) Key() string {
	if e.AggregateID != "" {
		return e.AggregateID
	}
	return e.EventID
}

// Validate checks required fields.
func (e Envelope) Validate() error {
	switch {
	case e.EventID == "":
		return errors.New("envelope: event_id is required")
	case e.EventType == "":
		return errors.New("envelope: event_type is required")
	case e.EventVersion < 1:
		return errors.New("envelope: event_version must be >= 1")
	case len(e.Payload) == 0:
		return errors.New("envelope: payload is required")
	}
	return nil
}

// Scope returns the envelope's scope.
func (e Envelope) Scope() reqctx.Scope {
	return reqctx.Scope{CompanyID: e.CompanyID, VerticalID: e.VerticalID, TenantID: e.TenantID}
}

// Decode unmarshals the payload.
func (e Envelope) Decode(dst any) error {
	if err := json.Unmarshal(e.Payload, dst); err != nil {
		return Permanent(fmt.Errorf("decode %s v%d payload: %w", e.EventType, e.EventVersion, err))
	}
	return nil
}

// Context returns ctx enriched with the envelope's correlation ID, scope and
// actor, so logs and downstream events continue the same trail.
func (e Envelope) Context(ctx context.Context) context.Context {
	if e.CorrelationID != "" {
		ctx = reqctx.WithCorrelationID(ctx, e.CorrelationID)
	}
	if s := e.Scope(); !s.IsZero() {
		ctx = reqctx.WithScope(ctx, s)
	}
	if e.Actor != nil {
		ctx = reqctx.WithActor(ctx, *e.Actor)
	}
	return ctx
}
