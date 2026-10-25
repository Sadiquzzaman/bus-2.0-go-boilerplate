// Package mongooutbox is the MongoDB implementation of the outbox: the
// writer used inside mongo.DB.WithTx, the relay Store and a lease-based
// Leader. Published documents are removed by a TTL index.
package mongooutbox

import (
	"context"
	"fmt"
	"os"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/Sadiquzzaman/bus-2.0-go-boilerplate/platform/kafka"
	"github.com/Sadiquzzaman/bus-2.0-go-boilerplate/platform/outbox"
)

// Collection names.
const (
	Collection      = "outbox_events"
	LocksCollection = "platform_locks"
)

type doc struct {
	ID          string            `bson:"_id"`
	Topic       string            `bson:"topic"`
	Key         string            `bson:"key"`
	Payload     string            `bson:"payload"`
	Headers     map[string]string `bson:"headers"`
	CreatedAt   time.Time         `bson:"created_at"`
	PublishedAt *time.Time        `bson:"published_at"`
	Attempts    int               `bson:"attempts"`
	LastError   string            `bson:"last_error,omitempty"`
}

// EnsureIndexes creates the indexes the outbox needs. retention controls the
// TTL of published documents. Call once at startup.
func EnsureIndexes(ctx context.Context, db *mongo.Database, retention time.Duration) error {
	_, err := db.Collection(Collection).Indexes().CreateMany(ctx, []mongo.IndexModel{
		{Keys: bson.D{{Key: "published_at", Value: 1}, {Key: "created_at", Value: 1}}},
		{
			Keys:    bson.D{{Key: "published_at", Value: 1}},
			Options: options.Index().SetName("published_at_ttl").SetExpireAfterSeconds(int32(retention.Seconds())),
		},
	})
	if err != nil {
		return fmt.Errorf("mongooutbox: indexes: %w", err)
	}
	return nil
}

// Writer inserts outbox documents. Call Add with the ctx received inside
// mongo.DB.WithTx so the insert joins the business transaction.
type Writer struct {
	Service string
	DB      *mongo.Database
}

func (w Writer) Add(ctx context.Context, topic string, envs ...kafka.Envelope) error {
	docs := make([]any, 0, len(envs))
	for _, env := range envs {
		row, err := outbox.NewRow(ctx, w.Service, topic, env)
		if err != nil {
			return err
		}
		docs = append(docs, doc{
			ID: row.ID, Topic: row.Topic, Key: row.Key, Payload: string(row.Payload),
			Headers: row.Headers, CreatedAt: time.Now().UTC(),
		})
	}
	if _, err := w.DB.Collection(Collection).InsertMany(ctx, docs); err != nil {
		return fmt.Errorf("mongooutbox: insert: %w", err)
	}
	return nil
}

// Store is the relay Store for MongoDB.
type Store struct{ DB *mongo.Database }

func (s Store) coll() *mongo.Collection { return s.DB.Collection(Collection) }

func (s Store) Fetch(ctx context.Context, limit int) ([]outbox.Row, error) {
	cur, err := s.coll().Find(ctx, bson.D{{Key: "published_at", Value: nil}},
		options.Find().SetSort(bson.D{{Key: "created_at", Value: 1}, {Key: "_id", Value: 1}}).SetLimit(int64(limit)))
	if err != nil {
		return nil, fmt.Errorf("mongooutbox: fetch: %w", err)
	}
	defer cur.Close(ctx)
	var out []outbox.Row
	for cur.Next(ctx) {
		var d doc
		if err := cur.Decode(&d); err != nil {
			return nil, err
		}
		out = append(out, outbox.Row{ID: d.ID, Topic: d.Topic, Key: d.Key, Payload: []byte(d.Payload), Headers: d.Headers, Attempts: d.Attempts})
	}
	return out, cur.Err()
}

func (s Store) MarkPublished(ctx context.Context, ids []string) error {
	if len(ids) == 0 {
		return nil
	}
	_, err := s.coll().UpdateMany(ctx,
		bson.D{{Key: "_id", Value: bson.D{{Key: "$in", Value: ids}}}},
		bson.D{{Key: "$set", Value: bson.D{{Key: "published_at", Value: time.Now().UTC()}}}})
	return err
}

func (s Store) MarkFailed(ctx context.Context, id string, cause error) error {
	_, err := s.coll().UpdateByID(ctx, id, bson.D{
		{Key: "$inc", Value: bson.D{{Key: "attempts", Value: 1}}},
		{Key: "$set", Value: bson.D{{Key: "last_error", Value: cause.Error()}}},
	})
	return err
}

func (s Store) Pending(ctx context.Context) (int64, error) {
	return s.coll().CountDocuments(ctx, bson.D{{Key: "published_at", Value: nil}})
}

// Cleanup is a no-op: the TTL index removes published documents.
func (s Store) Cleanup(context.Context, time.Time) (int64, error) { return 0, nil }

// Leader is a lease stored in LocksCollection, renewed every TTL/3.
type Leader struct {
	DB   *mongo.Database
	Name string
	TTL  time.Duration // default 15s
}

func (l Leader) Acquire(ctx context.Context) (context.Context, func(), error) {
	ttl := l.TTL
	if ttl <= 0 {
		ttl = 15 * time.Second
	}
	host, _ := os.Hostname()
	owner := fmt.Sprintf("%s-%d", host, time.Now().UnixNano())
	coll := l.DB.Collection(LocksCollection)

	try := func(ctx context.Context) (bool, error) {
		now := time.Now().UTC()
		filter := bson.D{{Key: "_id", Value: l.Name}, {Key: "$or", Value: bson.A{
			bson.D{{Key: "expires_at", Value: bson.D{{Key: "$lt", Value: now}}}},
			bson.D{{Key: "owner", Value: owner}},
		}}}
		update := bson.D{{Key: "$set", Value: bson.D{{Key: "owner", Value: owner}, {Key: "expires_at", Value: now.Add(ttl)}}}}
		_, err := coll.UpdateOne(ctx, filter, update, options.UpdateOne().SetUpsert(true))
		if mongo.IsDuplicateKeyError(err) {
			return false, nil // held by someone else
		}
		return err == nil, err
	}

	for {
		ok, err := try(ctx)
		if err != nil {
			return nil, nil, err
		}
		if ok {
			break
		}
		outbox.Sleep(ctx, ttl/3)
		if ctx.Err() != nil {
			return nil, nil, ctx.Err()
		}
	}

	lead, cancel := context.WithCancel(ctx)
	go func() {
		t := time.NewTicker(ttl / 3)
		defer t.Stop()
		for {
			select {
			case <-lead.Done():
				return
			case <-t.C:
				if ok, err := try(lead); err != nil || !ok {
					cancel()
					return
				}
			}
		}
	}()
	release := func() {
		cancel()
		rctx, c := context.WithTimeout(context.Background(), 5*time.Second)
		defer c()
		_, _ = coll.DeleteOne(rctx, bson.D{{Key: "_id", Value: l.Name}, {Key: "owner", Value: owner}})
	}
	return lead, release, nil
}

var (
	_ outbox.Store  = Store{}
	_ outbox.Leader = Leader{}
)
