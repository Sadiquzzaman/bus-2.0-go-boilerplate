package redis

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

// ---------------------------------------------------------------- cache

// GetJSON loads key into dst. found is false when the key does not exist.
func (c *Client) GetJSON(ctx context.Context, key string, dst any) (found bool, err error) {
	b, err := c.Get(ctx, key).Bytes()
	if IsNil(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, json.Unmarshal(b, dst)
}

// SetJSON stores v as JSON with a TTL.
func (c *Client) SetJSON(ctx context.Context, key string, v any, ttl time.Duration) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return c.Set(ctx, key, b, ttl).Err()
}

// Remember returns the cached value for key or calls load, caches and
// returns its result. Cache errors never fail the call; load errors do.
func Remember[T any](ctx context.Context, c *Client, key string, ttl time.Duration, load func(context.Context) (T, error)) (T, error) {
	var v T
	if found, err := c.GetJSON(ctx, key, &v); err == nil && found {
		return v, nil
	}
	v, err := load(ctx)
	if err != nil {
		return v, err
	}
	_ = c.SetJSON(ctx, key, v, ttl)
	return v, nil
}

// ---------------------------------------------------------------- locks

// ErrLockNotAcquired is returned when another holder owns the lock.
var ErrLockNotAcquired = errors.New("redis: lock not acquired")

// Lock is a held distributed lock (single Redis instance semantics).
// Use it for short critical sections such as holding a seat; do not use it
// as the only guard for correctness — back it with a DB constraint.
type Lock struct {
	c     *Client
	key   string
	token string
}

var unlockScript = goredis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
  return redis.call("DEL", KEYS[1])
end
return 0`)

var extendScript = goredis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
  return redis.call("PEXPIRE", KEYS[1], ARGV[2])
end
return 0`)

// TryLock acquires key for ttl or returns ErrLockNotAcquired.
func (c *Client) TryLock(ctx context.Context, key string, ttl time.Duration) (*Lock, error) {
	tok := randomToken()
	ok, err := c.SetNX(ctx, key, tok, ttl).Result()
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, ErrLockNotAcquired
	}
	return &Lock{c: c, key: key, token: tok}, nil
}

// Unlock releases the lock if still held by us.
func (l *Lock) Unlock(ctx context.Context) error {
	return unlockScript.Run(ctx, l.c, []string{l.key}, l.token).Err()
}

// Extend renews the TTL if still held by us.
func (l *Lock) Extend(ctx context.Context, ttl time.Duration) (bool, error) {
	n, err := extendScript.Run(ctx, l.c, []string{l.key}, l.token, ttl.Milliseconds()).Int()
	return n == 1, err
}

func randomToken() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// ---------------------------------------------------------- rate limits

var windowScript = goredis.NewScript(`
local n = redis.call("INCR", KEYS[1])
if n == 1 then redis.call("PEXPIRE", KEYS[1], ARGV[1]) end
return {n, redis.call("PTTL", KEYS[1])}`)

// RateResult is the outcome of Allow.
type RateResult struct {
	Allowed    bool
	Count      int64
	Limit      int64
	RetryAfter time.Duration
}

// Allow counts one hit against key in a fixed window and reports whether it
// is within limit. The INCR and EXPIRE run atomically in one script, so
// concurrent requests cannot slip past the limit.
func (c *Client) Allow(ctx context.Context, key string, limit int64, window time.Duration) (RateResult, error) {
	res, err := windowScript.Run(ctx, c, []string{key}, window.Milliseconds()).Int64Slice()
	if err != nil {
		return RateResult{}, fmt.Errorf("redis: rate limit: %w", err)
	}
	r := RateResult{Count: res[0], Limit: limit, Allowed: res[0] <= limit}
	if !r.Allowed && res[1] > 0 {
		r.RetryAfter = time.Duration(res[1]) * time.Millisecond
	}
	return r, nil
}
