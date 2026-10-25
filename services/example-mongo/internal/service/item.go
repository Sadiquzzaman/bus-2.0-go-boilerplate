// Package service is example-mongo's business logic. It depends only on the
// Store interfaces (and Kafka envelopes), never on Gin, ent or Mongo.
package service

import (
	"context"
	"strings"

	"github.com/Sadiquzzaman/bus-2.0-go-boilerplate/platform/apperror"
	"github.com/Sadiquzzaman/bus-2.0-go-boilerplate/platform/kafka"
	"github.com/Sadiquzzaman/bus-2.0-go-boilerplate/platform/reqctx"

	"github.com/Sadiquzzaman/bus-2.0-go-boilerplate/services/example-mongo/internal/domain"
	"github.com/Sadiquzzaman/bus-2.0-go-boilerplate/services/example-mongo/internal/events"
)

// ItemService implements the item use cases.
type ItemService struct {
	store Store
}

// NewItemService wires the service.
func NewItemService(store Store) *ItemService {
	return &ItemService{store: store}
}

// CreateItemInput is validated input for Create.
type CreateItemInput struct {
	Name        string
	Description string
}

// Create stores a new item in the caller's scope and records an
// item.created event in the same transaction.
func (s *ItemService) Create(ctx context.Context, in CreateItemInput) (domain.Item, error) {
	in.Name = strings.TrimSpace(in.Name)
	if in.Name == "" {
		return domain.Item{}, apperror.Validation(apperror.FieldError{Field: "name", Message: "is required"})
	}

	var out domain.Item
	err := s.store.Tx(ctx, func(ctx context.Context, r Repos) error {
		it, err := r.Items.Create(ctx, domain.Item{
			Name:        in.Name,
			Description: in.Description,
			Status:      domain.ItemActive,
		})
		if err != nil {
			return err
		}
		out = it
		return publish(ctx, r, events.ItemCreated, it)
	})
	return out, err
}

// Get returns one item.
func (s *ItemService) Get(ctx context.Context, id string) (domain.Item, error) {
	return s.store.Items().Get(ctx, id)
}

// List returns a page of items visible to the caller.
func (s *ItemService) List(ctx context.Context, f domain.ItemFilter) ([]domain.Item, int, error) {
	return s.store.Items().List(ctx, f)
}

// Archive changes status instead of deleting (records are never deleted).
func (s *ItemService) Archive(ctx context.Context, id string) (domain.Item, error) {
	var out domain.Item
	err := s.store.Tx(ctx, func(ctx context.Context, r Repos) error {
		cur, err := r.Items.Get(ctx, id)
		if err != nil {
			return err
		}
		if cur.Status == domain.ItemArchived {
			return apperror.Conflict("item is already archived")
		}
		it, err := r.Items.SetStatus(ctx, id, domain.ItemArchived)
		if err != nil {
			return err
		}
		out = it
		return publish(ctx, r, events.ItemArchived, it)
	})
	return out, err
}

func publish(ctx context.Context, r Repos, eventType string, it domain.Item) error {
	env, err := kafka.NewEnvelope(ctx, kafka.Event{
		Type:          eventType,
		Version:       1,
		AggregateType: events.EntityItem,
		AggregateID:   it.ID,
		Payload:       events.NewItemPayload(it),
	})
	if err != nil {
		return err
	}
	return r.Outbox.Add(ctx, events.TopicItem, env)
}

var _ = reqctx.ScopeFrom
