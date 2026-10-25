package service

import (
	"context"

	"github.com/Sadiquzzaman/bus-2.0-go-boilerplate/platform/kafka"
	"github.com/Sadiquzzaman/bus-2.0-go-boilerplate/services/example-pg/internal/domain"
)

// ItemRepository persists items. Implementations scope every call to the
// company in ctx (see platform/postgres/entx.ScopeMixin, platform/mongo.Scoped).
type ItemRepository interface {
	Create(ctx context.Context, it domain.Item) (domain.Item, error)
	Get(ctx context.Context, id string) (domain.Item, error) // apperror NOT_FOUND
	List(ctx context.Context, f domain.ItemFilter) ([]domain.Item, int, error)
	SetStatus(ctx context.Context, id, status string) (domain.Item, error)
}

// Outbox stores events in the current transaction; the relay publishes them.
type Outbox interface {
	Add(ctx context.Context, topic string, envs ...kafka.Envelope) error
}

// Inbox makes event handlers idempotent: Claim returns duplicate=true if
// this consumer group already processed the event.
type Inbox interface {
	Claim(ctx context.Context, group, eventID string) (duplicate bool, err error)
}

// Repos are the repositories bound to one transaction.
type Repos struct {
	Items  ItemRepository
	Outbox Outbox
	Inbox  Inbox
}

// Store is the persistence boundary. The service layer only sees these
// interfaces, so it is identical for Postgres and Mongo implementations and
// is unit-tested with fakes.
type Store interface {
	// Items is for reads outside a transaction.
	Items() ItemRepository
	// Tx runs fn in a transaction. Use the ctx passed to fn for every call.
	Tx(ctx context.Context, fn func(ctx context.Context, r Repos) error) error
}
