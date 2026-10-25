// Package pginbox makes Kafka handlers idempotent on Postgres.
//
// Inside the handler's transaction, claim the event first:
//
//	err := withTx(ctx, client, func(tx *ent.Tx) error {
//	    dup, err := pginbox.Claim(ctx, tx, group, msg.EventID)
//	    if err != nil || dup {
//	        return err // dup: already processed, nothing to do
//	    }
//	    ... do the work with tx ...
//	})
//
// The claim and the work commit together; if the work fails the claim rolls
// back and the redelivered message is processed again.
package pginbox

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/Sadiquzzaman/bus-2.0-go-boilerplate/platform/postgres/entx"
)

// Execer is satisfied by *sql.Tx and an ent *Tx built with sql/execquery.
type Execer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

// Claim records (group, eventID). It returns duplicate=true if the event was
// already processed by this consumer group.
func Claim(ctx context.Context, ex Execer, group, eventID string) (duplicate bool, err error) {
	res, err := ex.ExecContext(ctx,
		`INSERT INTO `+entx.InboxTable+` (id, consumer_group, event_id, processed_at)
		 VALUES ($1, $2, $3, now())
		 ON CONFLICT (consumer_group, event_id) DO NOTHING`,
		uuid.Must(uuid.NewV7()).String(), group, eventID)
	if err != nil {
		return false, fmt.Errorf("inbox: claim: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	return n == 0, nil
}

// Cleanup deletes claims older than before (run from a worker; keep them
// longer than the topics' retention).
func Cleanup(ctx context.Context, db *sql.DB, before time.Time) (int64, error) {
	res, err := db.ExecContext(ctx, `DELETE FROM `+entx.InboxTable+` WHERE processed_at < $1`, before)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}
