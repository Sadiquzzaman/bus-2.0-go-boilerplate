// Package pgoutbox is the Postgres implementation of the outbox: the writer
// used inside ent transactions, the relay Store and the advisory-lock Leader.
package pgoutbox

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"strings"
	"time"

	"github.com/Sadiquzzaman/bus-2.0-go-boilerplate/platform/kafka"
	"github.com/Sadiquzzaman/bus-2.0-go-boilerplate/platform/outbox"
	"github.com/Sadiquzzaman/bus-2.0-go-boilerplate/platform/postgres/entx"
)

// Execer is satisfied by *sql.DB, *sql.Tx and an ent *Tx generated with
// `--feature sql/execquery`.
type Execer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

// Writer writes outbox rows in Postgres.
type Writer struct{ Service string }

// Add stores envelopes for topic using ex — pass the business transaction so
// the event is committed atomically with the data change.
func (w Writer) Add(ctx context.Context, ex Execer, topic string, envs ...kafka.Envelope) error {
	for _, env := range envs {
		row, err := outbox.NewRow(ctx, w.Service, topic, env)
		if err != nil {
			return err
		}
		h, err := outbox.HeadersJSON(row.Headers)
		if err != nil {
			return err
		}
		_, err = ex.ExecContext(ctx,
			`INSERT INTO `+entx.OutboxTable+` (id, topic, key, payload, headers, created_at, attempts)
			 VALUES ($1, $2, $3, $4::jsonb, $5::jsonb, now(), 0)`,
			row.ID, row.Topic, row.Key, string(row.Payload), string(h))
		if err != nil {
			return fmt.Errorf("outbox: insert: %w", err)
		}
	}
	return nil
}

// Store is the relay Store for Postgres.
type Store struct{ DB *sql.DB }

func (s Store) Fetch(ctx context.Context, limit int) ([]outbox.Row, error) {
	rows, err := s.DB.QueryContext(ctx,
		`SELECT id::text, topic, key, payload::text, COALESCE(headers::text, '{}'), attempts
		   FROM `+entx.OutboxTable+`
		  WHERE published_at IS NULL
		  ORDER BY created_at, id
		  LIMIT $1`, limit)
	if err != nil {
		return nil, fmt.Errorf("outbox: fetch: %w", err)
	}
	defer rows.Close()
	var out []outbox.Row
	for rows.Next() {
		var r outbox.Row
		var payload, headers string
		if err := rows.Scan(&r.ID, &r.Topic, &r.Key, &payload, &headers, &r.Attempts); err != nil {
			return nil, err
		}
		r.Payload = []byte(payload)
		if err := json.Unmarshal([]byte(headers), &r.Headers); err != nil {
			return nil, fmt.Errorf("outbox: headers of %s: %w", r.ID, err)
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func (s Store) MarkPublished(ctx context.Context, ids []string) error {
	if len(ids) == 0 {
		return nil
	}
	// Build $1,$2,… to stay independent of driver array support.
	ph := make([]string, len(ids))
	args := make([]any, len(ids))
	for i, id := range ids {
		ph[i] = fmt.Sprintf("$%d::uuid", i+1)
		args[i] = id
	}
	_, err := s.DB.ExecContext(ctx,
		`UPDATE `+entx.OutboxTable+` SET published_at = now() WHERE id IN (`+strings.Join(ph, ",")+`)`, args...)
	return err
}

func (s Store) MarkFailed(ctx context.Context, id string, cause error) error {
	msg := cause.Error()
	if len(msg) > 2000 {
		msg = msg[:2000]
	}
	_, err := s.DB.ExecContext(ctx,
		`UPDATE `+entx.OutboxTable+` SET attempts = attempts + 1, last_error = $2 WHERE id = $1::uuid`, id, msg)
	return err
}

func (s Store) Pending(ctx context.Context) (int64, error) {
	var n int64
	err := s.DB.QueryRowContext(ctx, `SELECT count(*) FROM `+entx.OutboxTable+` WHERE published_at IS NULL`).Scan(&n)
	return n, err
}

func (s Store) Cleanup(ctx context.Context, before time.Time) (int64, error) {
	var total int64
	for {
		res, err := s.DB.ExecContext(ctx,
			`DELETE FROM `+entx.OutboxTable+` WHERE id IN (
			   SELECT id FROM `+entx.OutboxTable+` WHERE published_at < $1 LIMIT 5000)`, before)
		if err != nil {
			return total, err
		}
		n, _ := res.RowsAffected()
		total += n
		if n < 5000 || ctx.Err() != nil {
			return total, nil
		}
	}
}

// Leader elects the relay leader with a session-level advisory lock held on
// a dedicated connection. If the connection drops, leadership is lost.
type Leader struct {
	DB   *sql.DB
	Name string // lock name, e.g. "<service>-outbox-relay"
}

func (l Leader) key() int64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(l.Name))
	return int64(h.Sum64())
}

func (l Leader) Acquire(ctx context.Context) (context.Context, func(), error) {
	for {
		conn, err := l.DB.Conn(ctx)
		if err != nil {
			return nil, nil, err
		}
		var ok bool
		if err := conn.QueryRowContext(ctx, `SELECT pg_try_advisory_lock($1)`, l.key()).Scan(&ok); err != nil {
			_ = conn.Close()
			return nil, nil, err
		}
		if ok {
			lead, cancel := context.WithCancel(ctx)
			// Watch the connection; losing it means losing the lock.
			go func() {
				t := time.NewTicker(5 * time.Second)
				defer t.Stop()
				for {
					select {
					case <-lead.Done():
						return
					case <-t.C:
						if err := conn.PingContext(lead); err != nil {
							cancel()
							return
						}
					}
				}
			}()
			release := func() {
				cancel()
				uctx, c := context.WithTimeout(context.Background(), 5*time.Second)
				defer c()
				_, _ = conn.ExecContext(uctx, `SELECT pg_advisory_unlock($1)`, l.key())
				_ = conn.Close()
			}
			return lead, release, nil
		}
		_ = conn.Close()
		outbox.Sleep(ctx, 5*time.Second)
		if ctx.Err() != nil {
			return nil, nil, ctx.Err()
		}
	}
}

var (
	_ outbox.Store  = Store{}
	_ outbox.Leader = Leader{}
)
