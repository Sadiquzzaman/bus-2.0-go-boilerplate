// Package config is the example-mongo configuration, read from environment
// variables (see .env.example). Platform modules bring their own fields; add
// service-specific settings to Service.
package config

import (
	"github.com/Sadiquzzaman/bus-2.0-go-boilerplate/platform/auth"
	platformconfig "github.com/Sadiquzzaman/bus-2.0-go-boilerplate/platform/config"
	"github.com/Sadiquzzaman/bus-2.0-go-boilerplate/platform/httpserver"
	"github.com/Sadiquzzaman/bus-2.0-go-boilerplate/platform/kafka"
	"github.com/Sadiquzzaman/bus-2.0-go-boilerplate/platform/logger"
	"github.com/Sadiquzzaman/bus-2.0-go-boilerplate/platform/mongo"
	"github.com/Sadiquzzaman/bus-2.0-go-boilerplate/platform/observability"
	"github.com/Sadiquzzaman/bus-2.0-go-boilerplate/platform/outbox"
)

// Config is the full service configuration.
type Config struct {
	App           platformconfig.App   `envPrefix:"APP_"`
	Log           logger.Config        `envPrefix:"LOG_"`
	Observability observability.Config `envPrefix:"OTEL_"`
	HTTP          httpserver.Config    `envPrefix:"HTTP_"`
	Auth          auth.Config          `envPrefix:"AUTH_"`
	Mongo         mongo.Config         `envPrefix:"MONGO_"`
	Kafka         kafka.Config         `envPrefix:"KAFKA_"`
	Consumer      Consumer             `envPrefix:"CONSUMER_"`
	Outbox        outbox.RelayConfig   `envPrefix:"OUTBOX_"`
	Service       Service              `envPrefix:"SERVICE_"`
}

// Consumer configures this service's consumer group.
type Consumer struct {
	// Topics to consume. Defaults to this service's own item topic, which
	// is only useful as a demo — point it at the topics you depend on.
	Topics []string `env:"TOPICS" envSeparator:","`
}

// Service holds example-mongo-specific settings.
type Service struct {
	// Add fields here, e.g.:
	// SeatHoldTTL time.Duration `env:"SEAT_HOLD_TTL" envDefault:"10m"`
}

// Validate is called by platformconfig.Load.
func (c Config) Validate() error {
	return platformconfig.Join(c.Auth, c.Kafka)
}
