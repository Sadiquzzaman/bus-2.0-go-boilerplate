// Package mongo connects to MongoDB (official driver v2) and provides
// transactions and company scoping for services that use Mongo instead of
// Postgres.
//
// Transactions (needed by the outbox and inbox) require a replica set — the
// local docker-compose runs a single-node replica set for that reason.
package mongo

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"

	"github.com/Sadiquzzaman/bus-2.0-go-boilerplate/platform/reqctx"
)

// Config for the client.
type Config struct {
	URI         string        `env:"URI,required"` // mongodb://host:27017/?replicaSet=rs0
	Database    string        `env:"DATABASE,required"`
	MaxPoolSize uint64        `env:"MAX_POOL_SIZE" envDefault:"50"`
	Timeout     time.Duration `env:"TIMEOUT" envDefault:"10s"`
}

// DB is a connected client plus the service database.
type DB struct {
	Client *mongo.Client
	DB     *mongo.Database
}

// Open connects and pings the primary.
func Open(ctx context.Context, cfg Config) (*DB, error) {
	opts := options.Client().
		ApplyURI(cfg.URI).
		SetMaxPoolSize(cfg.MaxPoolSize).
		SetTimeout(cfg.Timeout).
		SetWriteConcern(writeconcern.Majority())
	c, err := mongo.Connect(opts)
	if err != nil {
		return nil, fmt.Errorf("mongo: connect: %w", err)
	}
	pctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := c.Ping(pctx, readpref.Primary()); err != nil {
		_ = c.Disconnect(context.Background())
		return nil, fmt.Errorf("mongo: ping: %w", err)
	}
	return &DB{Client: c, DB: c.Database(cfg.Database)}, nil
}

// Check is a health.CheckFunc.
func (d *DB) Check(ctx context.Context) error { return d.Client.Ping(ctx, readpref.Primary()) }

// Shutdown disconnects (signature fits app.OnClose).
func (d *DB) Shutdown(ctx context.Context) error { return d.Client.Disconnect(ctx) }

// WithTx runs fn in a multi-document transaction. Pass the ctx given to fn
// to every collection call so they join the transaction. The driver retries
// fn on transient transaction errors, so fn must be safe to re-run.
func (d *DB) WithTx(ctx context.Context, fn func(ctx context.Context) error) error {
	sess, err := d.Client.StartSession()
	if err != nil {
		return fmt.Errorf("mongo: start session: %w", err)
	}
	defer sess.EndSession(ctx)
	_, err = sess.WithTransaction(ctx, func(sctx context.Context) (any, error) {
		return nil, fn(sctx)
	})
	return err
}

// ErrMissingScope is returned when a scoped collection is used without a
// company in the context.
var ErrMissingScope = errors.New("mongo: company scope missing from context")

// Field names used for scoping (same as the Postgres columns).
const (
	FieldCompanyID  = "company_id"
	FieldVerticalID = "vertical_id"
	FieldTenantID   = "tenant_id"
)

// Scoped returns filter with company_id from the context added. Use it for
// every find/update/delete on scoped collections:
//
//	f, err := mongo.Scoped(ctx, bson.D{{Key: "_id", Value: id}})
//	coll.FindOne(ctx, f)
//
// It returns ErrMissingScope unless reqctx.WithoutScopeFilter was applied.
func Scoped(ctx context.Context, filter bson.D) (bson.D, error) {
	if reqctx.ScopeFilterDisabled(ctx) {
		return filter, nil
	}
	s, ok := reqctx.ScopeFrom(ctx)
	if !ok || s.CompanyID == "" {
		return nil, ErrMissingScope
	}
	return append(bson.D{{Key: FieldCompanyID, Value: s.CompanyID}}, filter...), nil
}

// ScopeFields returns the scope fields to embed in a new document.
func ScopeFields(ctx context.Context) (bson.D, error) {
	s, ok := reqctx.ScopeFrom(ctx)
	if !ok || s.CompanyID == "" {
		return nil, ErrMissingScope
	}
	d := bson.D{{Key: FieldCompanyID, Value: s.CompanyID}}
	if s.VerticalID != "" {
		d = append(d, bson.E{Key: FieldVerticalID, Value: s.VerticalID})
	}
	if s.TenantID != "" {
		d = append(d, bson.E{Key: FieldTenantID, Value: s.TenantID})
	}
	return d, nil
}

// VisibleTo is the Mongo equivalent of entx.VisibleTo.
func VisibleTo(verticalIDs, tenantIDs []string) bson.E {
	var or bson.A
	if len(verticalIDs) > 0 {
		or = append(or, bson.D{{Key: FieldVerticalID, Value: bson.D{{Key: "$in", Value: verticalIDs}}}})
	}
	if len(tenantIDs) > 0 {
		or = append(or, bson.D{{Key: FieldTenantID, Value: bson.D{{Key: "$in", Value: tenantIDs}}}})
	}
	if len(or) == 0 {
		// Matches nothing.
		return bson.E{Key: "_id", Value: bson.D{{Key: "$exists", Value: false}}}
	}
	return bson.E{Key: "$or", Value: or}
}

// IsDuplicateKey reports a unique index violation.
func IsDuplicateKey(err error) bool { return mongo.IsDuplicateKeyError(err) }

// IsNotFound reports a FindOne with no result.
func IsNotFound(err error) bool { return errors.Is(err, mongo.ErrNoDocuments) }
