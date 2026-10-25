// Package entx holds ent mixins and helpers shared by every Postgres service.
//
//	func (Trip) Mixin() []ent.Mixin {
//	    return []ent.Mixin{entx.IDMixin{}, entx.TimeMixin{}, entx.ScopeMixin{}}
//	}
//
// Conventions enforced here:
//   - IDs are UUIDv7 generated in Go (time-ordered, index friendly).
//   - Every table has created_at / updated_at.
//   - Business records are not deleted; model a status field instead.
//   - Scoped tables carry company_id (+ optional vertical_id, tenant_id) and
//     every query/update/delete is automatically limited to the request's
//     company. Missing scope is an error (fail closed).
package entx

import (
	"context"
	"errors"
	"fmt"
	"time"

	"entgo.io/ent"
	"entgo.io/ent/dialect/sql"
	"entgo.io/ent/schema/field"
	"entgo.io/ent/schema/index"
	"entgo.io/ent/schema/mixin"
	"github.com/google/uuid"

	"github.com/Sadiquzzaman/bus-2.0-go-boilerplate/platform/reqctx"
)

// NewID returns a new UUIDv7.
func NewID() uuid.UUID { return uuid.Must(uuid.NewV7()) }

// IDMixin adds a UUIDv7 primary key named id.
type IDMixin struct{ mixin.Schema }

func (IDMixin) Fields() []ent.Field {
	return []ent.Field{
		field.UUID("id", uuid.UUID{}).Default(NewID).Immutable(),
	}
}

// TimeMixin adds created_at and updated_at.
type TimeMixin struct{ mixin.Schema }

func (TimeMixin) Fields() []ent.Field {
	return []ent.Field{
		field.Time("created_at").Default(time.Now).Immutable(),
		field.Time("updated_at").Default(time.Now).UpdateDefault(time.Now),
	}
}

// Column names used by ScopeMixin.
const (
	ColCompanyID  = "company_id"
	ColVerticalID = "vertical_id"
	ColTenantID   = "tenant_id"
)

// ErrMissingScope is returned when a scoped table is touched without a
// company in the context.
var ErrMissingScope = errors.New("entx: company scope missing from context")

// ScopeMixin adds company_id / vertical_id / tenant_id and enforces company
// isolation:
//   - create: company_id (and vertical/tenant if unset) is filled from the
//     context scope; an explicit different company_id is rejected.
//   - query/update/delete: WHERE company_id = <scope company> is added.
//
// Use reqctx.WithoutScopeFilter(ctx) for system jobs only.
// Vertical/tenant visibility depends on the permission being checked, so it is
// NOT automatic: filter list queries with VisibleTo (see predicates.go).
type ScopeMixin struct{ mixin.Schema }

func (ScopeMixin) Fields() []ent.Field {
	return []ent.Field{
		field.UUID(ColCompanyID, uuid.UUID{}).Immutable(),
		field.UUID(ColVerticalID, uuid.UUID{}).Optional().Nillable(),
		field.UUID(ColTenantID, uuid.UUID{}).Optional().Nillable(),
	}
}

func (ScopeMixin) Indexes() []ent.Index {
	return []ent.Index{
		index.Fields(ColCompanyID, ColVerticalID, ColTenantID),
	}
}

// wherer is implemented by every ent-generated query and mutation.
type wherer interface {
	WhereP(...func(*sql.Selector))
}

func (ScopeMixin) Interceptors() []ent.Interceptor {
	return []ent.Interceptor{
		ent.TraverseFunc(func(ctx context.Context, q ent.Query) error {
			if reqctx.ScopeFilterDisabled(ctx) {
				return nil
			}
			cid, err := companyFrom(ctx)
			if err != nil {
				return err
			}
			w, ok := q.(wherer)
			if !ok {
				return fmt.Errorf("entx: query %T does not support WhereP", q)
			}
			w.WhereP(sql.FieldEQ(ColCompanyID, cid))
			return nil
		}),
	}
}

func (ScopeMixin) Hooks() []ent.Hook {
	return []ent.Hook{
		func(next ent.Mutator) ent.Mutator {
			return ent.MutateFunc(func(ctx context.Context, m ent.Mutation) (ent.Value, error) {
				if reqctx.ScopeFilterDisabled(ctx) {
					return next.Mutate(ctx, m)
				}
				s, _ := reqctx.ScopeFrom(ctx)
				cid, err := companyFrom(ctx)
				if err != nil {
					return nil, err
				}
				switch {
				case m.Op().Is(ent.OpCreate):
					if v, ok := m.Field(ColCompanyID); ok {
						if id, _ := v.(uuid.UUID); id != cid {
							return nil, fmt.Errorf("entx: company_id %v differs from request scope", v)
						}
					} else if err := m.SetField(ColCompanyID, cid); err != nil {
						return nil, err
					}
					if err := setOptional(m, ColVerticalID, s.VerticalID); err != nil {
						return nil, err
					}
					if err := setOptional(m, ColTenantID, s.TenantID); err != nil {
						return nil, err
					}
				default: // update, update-one, delete, delete-one
					w, ok := m.(wherer)
					if !ok {
						return nil, fmt.Errorf("entx: mutation %T does not support WhereP", m)
					}
					w.WhereP(sql.FieldEQ(ColCompanyID, cid))
				}
				return next.Mutate(ctx, m)
			})
		},
	}
}

func companyFrom(ctx context.Context) (uuid.UUID, error) {
	s, ok := reqctx.ScopeFrom(ctx)
	if !ok || s.CompanyID == "" {
		return uuid.Nil, ErrMissingScope
	}
	id, err := uuid.Parse(s.CompanyID)
	if err != nil {
		return uuid.Nil, fmt.Errorf("entx: invalid company_id in scope: %w", err)
	}
	return id, nil
}

func setOptional(m ent.Mutation, col, v string) error {
	if v == "" {
		return nil
	}
	if _, ok := m.Field(col); ok {
		return nil
	}
	id, err := uuid.Parse(v)
	if err != nil {
		return fmt.Errorf("entx: invalid %s in scope: %w", col, err)
	}
	return m.SetField(col, id)
}
