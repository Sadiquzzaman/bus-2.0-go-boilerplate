// Package kafka is the messaging layer: a standard JSON event envelope, a
// producer, and a consumer with manual commits, retry topics and a
// dead-letter topic. Built on franz-go (pure Go, no cgo).
//
// Conventions (see docs/conventions.md):
//
//	topic name      <service>.<entity>.v<N>          booking.seat.v1
//	event type      <entity>.<past-tense-verb>       seat.reserved
//	message key     aggregate ID                      (keeps per-entity order)
//	retry topics    <topic>.<group>.retry.<n>
//	dead letters    <topic>.<group>.dlt
//
// Topics are never auto-created at runtime. In shared environments DevOps
// creates them; `<svc> topics` creates them locally.
package kafka

import (
	"crypto/tls"
	"fmt"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

// Config for brokers and security. Shared by producer and consumers.
type Config struct {
	Brokers        []string      `env:"BROKERS" envSeparator:"," envDefault:"localhost:9092"`
	ClientID       string        `env:"CLIENT_ID"`      // defaults to the service name
	SASLMechanism  string        `env:"SASL_MECHANISM"` // "" | PLAIN | SCRAM-SHA-256 | SCRAM-SHA-512
	SASLUsername   string        `env:"SASL_USERNAME"`
	SASLPassword   string        `env:"SASL_PASSWORD"`
	TLS            bool          `env:"TLS" envDefault:"false"`
	ProduceTimeout time.Duration `env:"PRODUCE_TIMEOUT" envDefault:"10s"`
	// Defaults for topics created by EnsureTopics (local/dev only).
	DefaultPartitions  int32 `env:"DEFAULT_PARTITIONS" envDefault:"12"`
	DefaultReplication int16 `env:"DEFAULT_REPLICATION" envDefault:"1"`
}

// Validate checks the SASL settings.
func (c Config) Validate() error {
	switch strings.ToUpper(c.SASLMechanism) {
	case "", "PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512":
	default:
		return fmt.Errorf("kafka: unsupported SASL mechanism %q", c.SASLMechanism)
	}
	if c.SASLMechanism != "" && (c.SASLUsername == "" || c.SASLPassword == "") {
		return fmt.Errorf("kafka: SASL username and password are required")
	}
	return nil
}

// baseOpts returns connection options common to every client.
func (c Config) baseOpts(clientID string) []kgo.Opt {
	if c.ClientID != "" {
		clientID = c.ClientID
	}
	opts := []kgo.Opt{
		kgo.SeedBrokers(c.Brokers...),
		kgo.ClientID(clientID),
	}
	if c.TLS {
		opts = append(opts, kgo.DialTLSConfig(&tls.Config{MinVersion: tls.VersionTLS12}))
	}
	switch strings.ToUpper(c.SASLMechanism) {
	case "PLAIN":
		opts = append(opts, kgo.SASL(plain.Auth{User: c.SASLUsername, Pass: c.SASLPassword}.AsMechanism()))
	case "SCRAM-SHA-256":
		opts = append(opts, kgo.SASL(scram.Auth{User: c.SASLUsername, Pass: c.SASLPassword}.AsSha256Mechanism()))
	case "SCRAM-SHA-512":
		opts = append(opts, kgo.SASL(scram.Auth{User: c.SASLUsername, Pass: c.SASLPassword}.AsSha512Mechanism()))
	}
	return opts
}

// Topic builds a topic name following the convention: Topic("booking","seat",1)
// → "booking.seat.v1".
func Topic(service, entity string, version int) string {
	return fmt.Sprintf("%s.%s.v%d", service, entity, version)
}

// RetryTopic returns the n-th (1-based) retry topic of topic for group.
func RetryTopic(topic, group string, n int) string {
	return fmt.Sprintf("%s.%s.retry.%d", topic, group, n)
}

// DLTopic returns the dead-letter topic of topic for group.
func DLTopic(topic, group string) string { return topic + "." + group + ".dlt" }
