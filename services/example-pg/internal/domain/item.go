// Package domain holds example-pg's business types. It has no dependencies on
// HTTP, Kafka or the database.
//
// "Item" is a sample entity showing every pattern (scoping, outbox events,
// caching, permissions). Rename or replace it with your real entities.
package domain

import "time"

// Item statuses. Business records are never deleted; they change status.
const (
	ItemActive   = "ACTIVE"
	ItemArchived = "ARCHIVED"
)

// Item is a company-scoped record.
type Item struct {
	ID          string    `json:"id"`
	CompanyID   string    `json:"company_id"`
	VerticalID  string    `json:"vertical_id,omitempty"`
	TenantID    string    `json:"tenant_id,omitempty"`
	Name        string    `json:"name"`
	Description string    `json:"description,omitempty"`
	Status      string    `json:"status"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}

// ItemFilter narrows a list query. Visibility comes from the caller's
// access list (whole company, or specific verticals/tenants).
type ItemFilter struct {
	Status       string
	WholeCompany bool
	VerticalIDs  []string
	TenantIDs    []string
	Offset       int
	Limit        int
}
