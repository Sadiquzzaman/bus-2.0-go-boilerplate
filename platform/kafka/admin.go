package kafka

import (
	"context"
	"errors"
	"fmt"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

// EnsureTopics creates missing topics with the configured default partitions
// and replication. Meant for local development (`<svc> topics`).
// In shared environments topics are created by DevOps; services must not
// create topics at runtime.
func EnsureTopics(ctx context.Context, cfg Config, service string, topics ...string) error {
	cl, err := kgo.NewClient(cfg.baseOpts(service + "-admin")...)
	if err != nil {
		return err
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)

	resp, err := adm.CreateTopics(ctx, cfg.DefaultPartitions, cfg.DefaultReplication, nil, topics...)
	if err != nil {
		return fmt.Errorf("kafka: create topics: %w", err)
	}
	var errs []error
	for _, r := range resp.Sorted() {
		if r.Err != nil && !errors.Is(r.Err, kerr.TopicAlreadyExists) {
			errs = append(errs, fmt.Errorf("%s: %w", r.Topic, r.Err))
		}
	}
	return errors.Join(errs...)
}
