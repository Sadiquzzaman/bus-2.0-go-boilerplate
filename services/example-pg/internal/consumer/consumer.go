// Package consumer holds example-pg's Kafka event handlers.
//
// Handlers must be idempotent: claim the event in the inbox inside the same
// transaction as the work, so a redelivered message is a no-op.
package consumer

import (
	"context"
	"log/slog"

	"github.com/Sadiquzzaman/bus-2.0-go-boilerplate/platform/kafka"

	"github.com/Sadiquzzaman/bus-2.0-go-boilerplate/services/example-pg/internal/events"
	"github.com/Sadiquzzaman/bus-2.0-go-boilerplate/services/example-pg/internal/service"
)

// Handlers processes events this service subscribes to.
type Handlers struct {
	store service.Store
	log   *slog.Logger
}

// New creates the handlers.
func New(store service.Store, log *slog.Logger) *Handlers {
	return &Handlers{store: store, log: log}
}

// Router maps event types to handlers. Unregistered types are skipped.
func (h *Handlers) Router() *kafka.Router {
	return kafka.NewRouter().
		Handle(events.ItemCreated, h.onItem).
		Handle(events.ItemArchived, h.onItem)
}

// onItem is a demo handler: replace it with real work, such as updating a
// local read model of another service's data.
func (h *Handlers) onItem(ctx context.Context, msg *kafka.Message) error {
	var p events.ItemPayload
	if err := msg.Decode(&p); err != nil {
		return err // Decode errors are permanent → dead-letter topic
	}
	return h.store.Tx(ctx, func(ctx context.Context, r service.Repos) error {
		dup, err := r.Inbox.Claim(ctx, events.ConsumerGroup, msg.EventID)
		if err != nil || dup {
			return err
		}
		// Do the real work here with r (same transaction as the claim).
		h.log.InfoContext(ctx, "item event processed",
			slog.String("event_type", msg.EventType), slog.String("item_id", p.ID), slog.String("producer", msg.Producer))
		return nil
	})
}
