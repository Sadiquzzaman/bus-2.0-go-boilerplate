package entx

import (
	"time"

	"entgo.io/ent"
	"entgo.io/ent/dialect"
	"entgo.io/ent/dialect/entsql"
	"entgo.io/ent/schema"
	"entgo.io/ent/schema/field"
	"entgo.io/ent/schema/index"
	"entgo.io/ent/schema/mixin"
	"github.com/google/uuid"
)

// Table names used by platform/outbox and platform/inbox raw SQL.
const (
	OutboxTable = "outbox_events"
	InboxTable  = "processed_events"
)

// OutboxMixin defines the transactional outbox table. Every service that
// publishes events declares, in ent/schema/outbox.go:
//
//	type OutboxEvent struct{ ent.Schema }
//	func (OutboxEvent) Mixin() []ent.Mixin { return []ent.Mixin{entx.OutboxMixin{}} }
//
// so the table is part of the service's migrations. Rows are written by
// outbox.PG inside the business transaction and published by the relay.
type OutboxMixin struct{ mixin.Schema }

func (OutboxMixin) Fields() []ent.Field {
	return []ent.Field{
		field.UUID("id", uuid.UUID{}).Default(NewID).Immutable(),
		field.String("topic").NotEmpty(),
		field.String("key").Default(""),
		field.Bytes("payload").SchemaType(map[string]string{dialect.Postgres: "jsonb"}),
		field.JSON("headers", map[string]string{}).Optional(),
		field.Time("created_at").Default(time.Now).Immutable(),
		field.Time("published_at").Optional().Nillable(),
		field.Int("attempts").Default(0),
		field.String("last_error").Optional(),
	}
}

func (OutboxMixin) Indexes() []ent.Index {
	return []ent.Index{
		// The relay scans unpublished rows in insertion order.
		index.Fields("created_at").Annotations(entsql.IndexWhere("published_at IS NULL")),
		index.Fields("published_at"),
	}
}

func (OutboxMixin) Annotations() []schema.Annotation {
	return []schema.Annotation{entsql.Annotation{Table: OutboxTable}}
}

// InboxMixin defines the processed-events table used for idempotent
// consumers. Declare it like OutboxMixin:
//
//	type ProcessedEvent struct{ ent.Schema }
//	func (ProcessedEvent) Mixin() []ent.Mixin { return []ent.Mixin{entx.InboxMixin{}} }
type InboxMixin struct{ mixin.Schema }

func (InboxMixin) Fields() []ent.Field {
	return []ent.Field{
		field.UUID("id", uuid.UUID{}).Default(NewID).Immutable(),
		field.String("consumer_group").NotEmpty(),
		field.String("event_id").NotEmpty(),
		field.Time("processed_at").Default(time.Now).Immutable(),
	}
}

func (InboxMixin) Indexes() []ent.Index {
	return []ent.Index{
		index.Fields("consumer_group", "event_id").Unique(),
		index.Fields("processed_at"),
	}
}

func (InboxMixin) Annotations() []schema.Annotation {
	return []schema.Annotation{entsql.Annotation{Table: InboxTable}}
}
