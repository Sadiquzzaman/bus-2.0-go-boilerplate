package entx

import (
	"entgo.io/ent/dialect/sql"
	"github.com/google/uuid"
)

// VisibleTo limits a query on a ScopeMixin table to the verticals and
// tenants the caller can see (from auth.AccessList). A vertical grant covers
// all tenants in that vertical.
//
//	al, _ := auth.AccessListFrom(ctx)
//	q := client.Trip.Query()
//	if !al.WholeCompany {
//	    q = q.Where(predicate.Trip(entx.VisibleTo(al.VerticalIDs, al.TenantIDs)))
//	}
func VisibleTo(verticalIDs, tenantIDs []string) func(*sql.Selector) {
	return func(s *sql.Selector) {
		var preds []*sql.Predicate
		if ids := toAny(verticalIDs); len(ids) > 0 {
			preds = append(preds, sql.In(s.C(ColVerticalID), ids...))
		}
		if ids := toAny(tenantIDs); len(ids) > 0 {
			preds = append(preds, sql.In(s.C(ColTenantID), ids...))
		}
		if len(preds) == 0 {
			s.Where(sql.False())
			return
		}
		s.Where(sql.Or(preds...))
	}
}

func toAny(ids []string) []any {
	out := make([]any, 0, len(ids))
	for _, id := range ids {
		if u, err := uuid.Parse(id); err == nil {
			out = append(out, u)
		}
	}
	return out
}
