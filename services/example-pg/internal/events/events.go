// Package events declares the Kafka contract of example-pg: topics it owns,
// event types and payload shapes. Keep JSON Schemas for these payloads in
// contracts/example-pg/ — other services code against those, not this file.
package events

import (
	"time"

	"github.com/Sadiquzzaman/bus-2.0-go-boilerplate/services/example-pg/internal/domain"
)

// Service name used in topic names.
const Service = "example-pg"

// Entities and topics owned by this service (<service>.<entity>.v<N>).
const (
	EntityItem = "item"
	TopicItem  = Service + "." + EntityItem + ".v1"
)

// Event types (<entity>.<past-tense-verb>).
const (
	ItemCreated  = "item.created"
	ItemArchived = "item.archived"
)

// ConsumerGroup is this service's consumer group for the demo consumer.
// Use one group per independent purpose: "<service>.<purpose>".
const ConsumerGroup = Service + ".item-events"

// OwnTopics lists the topics this service produces to.
var OwnTopics = []string{TopicItem}

// ItemPayload is the payload of item.* events, version 1.
// Additive changes only; breaking changes need ItemPayloadV2 and new
// event_version alongside v1.
type ItemPayload struct {
	ID          string    `json:"id"`
	Name        string    `json:"name"`
	Description string    `json:"description,omitempty"`
	Status      string    `json:"status"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}

// NewItemPayload maps the domain item to the public payload.
func NewItemPayload(it domain.Item) ItemPayload {
	return ItemPayload{
		ID: it.ID, Name: it.Name, Description: it.Description, Status: it.Status,
		CreatedAt: it.CreatedAt, UpdatedAt: it.UpdatedAt,
	}
}
