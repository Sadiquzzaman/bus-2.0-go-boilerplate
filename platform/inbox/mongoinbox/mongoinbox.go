// Package mongoinbox makes Kafka handlers idempotent on MongoDB.
//
//	err := db.WithTx(ctx, func(ctx context.Context) error {
//	    dup, err := mongoinbox.Claim(ctx, db.DB, group, msg.EventID)
//	    if err != nil || dup {
//	        return err
//	    }
//	    ... do the work with ctx ...
//	})
package mongoinbox

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// Collection holds processed event claims.
const Collection = "processed_events"

// EnsureIndexes creates the unique claim index and a TTL index that removes
// claims after retention (keep it longer than the topics' retention).
func EnsureIndexes(ctx context.Context, db *mongo.Database, retention time.Duration) error {
	_, err := db.Collection(Collection).Indexes().CreateMany(ctx, []mongo.IndexModel{
		{
			Keys:    bson.D{{Key: "consumer_group", Value: 1}, {Key: "event_id", Value: 1}},
			Options: options.Index().SetUnique(true),
		},
		{
			Keys:    bson.D{{Key: "processed_at", Value: 1}},
			Options: options.Index().SetExpireAfterSeconds(int32(retention.Seconds())),
		},
	})
	if err != nil {
		return fmt.Errorf("mongoinbox: indexes: %w", err)
	}
	return nil
}

// Claim records (group, eventID) using ctx (pass the transaction ctx).
// duplicate=true means the event was already processed.
//
// A duplicate-key error aborts a Mongo transaction, so Claim first looks the
// claim up and only then inserts. If two deliveries race, the loser's insert
// fails, its transaction aborts and the message is retried — on retry the
// lookup finds the claim and reports a duplicate.
func Claim(ctx context.Context, db *mongo.Database, group, eventID string) (duplicate bool, err error) {
	coll := db.Collection(Collection)
	filter := bson.D{{Key: "consumer_group", Value: group}, {Key: "event_id", Value: eventID}}
	err = coll.FindOne(ctx, filter).Err()
	switch {
	case err == nil:
		return true, nil
	case !errors.Is(err, mongo.ErrNoDocuments):
		return false, fmt.Errorf("mongoinbox: lookup: %w", err)
	}
	_, err = coll.InsertOne(ctx, bson.D{
		{Key: "consumer_group", Value: group},
		{Key: "event_id", Value: eventID},
		{Key: "processed_at", Value: time.Now().UTC()},
	})
	if err != nil {
		return false, fmt.Errorf("mongoinbox: claim: %w", err)
	}
	return false, nil
}
