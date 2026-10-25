package schema

import (
	"entgo.io/ent"
	"entgo.io/ent/schema/field"
	"entgo.io/ent/schema/index"

	"github.com/Sadiquzzaman/bus-2.0-go-boilerplate/platform/postgres/entx"
)

// Item is a sample company-scoped entity. Replace it with your own.
type Item struct{ ent.Schema }

// Mixin adds id (UUIDv7), created_at/updated_at and company scoping.
func (Item) Mixin() []ent.Mixin {
	return []ent.Mixin{entx.IDMixin{}, entx.TimeMixin{}, entx.ScopeMixin{}}
}

func (Item) Fields() []ent.Field {
	return []ent.Field{
		field.String("name").NotEmpty().MaxLen(200),
		field.String("description").Default("").MaxLen(2000),
		field.String("status").Default("ACTIVE"),
	}
}

func (Item) Indexes() []ent.Index {
	return []ent.Index{
		index.Fields("company_id", "status", "created_at"),
	}
}
