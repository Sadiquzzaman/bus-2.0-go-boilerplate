// Package postgres implements service.Store with ent on PostgreSQL.
package postgres

import (
	"context"
	"fmt"

	"github.com/google/uuid"

	"github.com/Sadiquzzaman/bus-2.0-go-boilerplate/platform/apperror"
	"github.com/Sadiquzzaman/bus-2.0-go-boilerplate/platform/inbox/pginbox"
	"github.com/Sadiquzzaman/bus-2.0-go-boilerplate/platform/kafka"
	"github.com/Sadiquzzaman/bus-2.0-go-boilerplate/platform/outbox/pgoutbox"
	"github.com/Sadiquzzaman/bus-2.0-go-boilerplate/platform/postgres/entx"

	"github.com/Sadiquzzaman/bus-2.0-go-boilerplate/services/example-pg/ent"
	"github.com/Sadiquzzaman/bus-2.0-go-boilerplate/services/example-pg/ent/item"
	"github.com/Sadiquzzaman/bus-2.0-go-boilerplate/services/example-pg/ent/predicate"
	_ "github.com/Sadiquzzaman/bus-2.0-go-boilerplate/services/example-pg/ent/runtime" // registers schema hooks/interceptors
	"github.com/Sadiquzzaman/bus-2.0-go-boilerplate/services/example-pg/internal/domain"
	"github.com/Sadiquzzaman/bus-2.0-go-boilerplate/services/example-pg/internal/service"
)

// Store implements service.Store.
type Store struct {
	client *ent.Client
	outbox pgoutbox.Writer
}

// New creates the store. service is the producer name stamped on events.
func New(client *ent.Client, serviceName string) *Store {
	return &Store{client: client, outbox: pgoutbox.Writer{Service: serviceName}}
}

// Items returns a repository outside any transaction.
func (s *Store) Items() service.ItemRepository { return itemRepo{c: s.client} }

// Tx runs fn in one database transaction.
func (s *Store) Tx(ctx context.Context, fn func(ctx context.Context, r service.Repos) error) (err error) {
	tx, err := s.client.Tx(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() {
		if v := recover(); v != nil {
			_ = tx.Rollback()
			panic(v)
		}
	}()
	repos := service.Repos{
		Items:  itemRepo{c: tx.Client()},
		Outbox: txOutbox{w: s.outbox, tx: tx},
		Inbox:  txInbox{tx: tx},
	}
	if err := fn(ctx, repos); err != nil {
		if rerr := tx.Rollback(); rerr != nil {
			return fmt.Errorf("%w (rollback: %v)", err, rerr)
		}
		return err
	}
	return tx.Commit()
}

type txOutbox struct {
	w  pgoutbox.Writer
	tx *ent.Tx
}

func (o txOutbox) Add(ctx context.Context, topic string, envs ...kafka.Envelope) error {
	return o.w.Add(ctx, o.tx, topic, envs...)
}

type txInbox struct{ tx *ent.Tx }

func (i txInbox) Claim(ctx context.Context, group, eventID string) (bool, error) {
	return pginbox.Claim(ctx, i.tx, group, eventID)
}

// itemRepo is company-scoped automatically by entx.ScopeMixin.
type itemRepo struct{ c *ent.Client }

func (r itemRepo) Create(ctx context.Context, in domain.Item) (domain.Item, error) {
	e, err := r.c.Item.Create().
		SetName(in.Name).
		SetDescription(in.Description).
		SetStatus(in.Status).
		Save(ctx)
	if err != nil {
		return domain.Item{}, mapErr(err)
	}
	return toDomain(e), nil
}

func (r itemRepo) Get(ctx context.Context, id string) (domain.Item, error) {
	uid, err := uuid.Parse(id)
	if err != nil {
		return domain.Item{}, apperror.NotFound("item not found")
	}
	e, err := r.c.Item.Get(ctx, uid)
	if err != nil {
		return domain.Item{}, mapErr(err)
	}
	return toDomain(e), nil
}

func (r itemRepo) List(ctx context.Context, f domain.ItemFilter) ([]domain.Item, int, error) {
	q := r.c.Item.Query()
	if f.Status != "" {
		q = q.Where(item.StatusEQ(f.Status))
	}
	if !f.WholeCompany {
		q = q.Where(predicate.Item(entx.VisibleTo(f.VerticalIDs, f.TenantIDs)))
	}
	total, err := q.Clone().Count(ctx)
	if err != nil {
		return nil, 0, mapErr(err)
	}
	rows, err := q.Order(ent.Desc(item.FieldCreatedAt), ent.Desc(item.FieldID)).
		Offset(f.Offset).Limit(f.Limit).All(ctx)
	if err != nil {
		return nil, 0, mapErr(err)
	}
	out := make([]domain.Item, len(rows))
	for i, e := range rows {
		out[i] = toDomain(e)
	}
	return out, total, nil
}

func (r itemRepo) SetStatus(ctx context.Context, id, status string) (domain.Item, error) {
	uid, err := uuid.Parse(id)
	if err != nil {
		return domain.Item{}, apperror.NotFound("item not found")
	}
	e, err := r.c.Item.UpdateOneID(uid).SetStatus(status).Save(ctx)
	if err != nil {
		return domain.Item{}, mapErr(err)
	}
	return toDomain(e), nil
}

func toDomain(e *ent.Item) domain.Item {
	it := domain.Item{
		ID:          e.ID.String(),
		CompanyID:   e.CompanyID.String(),
		Name:        e.Name,
		Description: e.Description,
		Status:      e.Status,
		CreatedAt:   e.CreatedAt,
		UpdatedAt:   e.UpdatedAt,
	}
	if e.VerticalID != nil {
		it.VerticalID = e.VerticalID.String()
	}
	if e.TenantID != nil {
		it.TenantID = e.TenantID.String()
	}
	return it
}

func mapErr(err error) error {
	switch {
	case ent.IsNotFound(err):
		return apperror.NotFound("item not found")
	case ent.IsConstraintError(err):
		return apperror.Conflict("item conflicts with an existing record").Wrap(err)
	case ent.IsValidationError(err):
		return apperror.BadRequest(err.Error())
	default:
		return err
	}
}
