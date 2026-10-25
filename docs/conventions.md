# Conventions

Rules every service follows. The platform kit enforces most of them; the rest
are reviewed in PRs.

## Communication

- **Writes between services go through Kafka.** A service never calls another
  service's API to change its data.
- **Reads between services use HTTP GET** through `platform/httpclient`
  (timeouts, retries on GET only, circuit breaker, service token, context
  propagation). Prefer keeping a local read model built from events when a read
  is on a hot path.
- A service owns its database. Nobody else reads or writes its tables or
  collections.

## Kafka

| Thing | Convention | Example |
|---|---|---|
| Topic | `<service>.<entity>.v<N>` | `booking.seat.v1` |
| Event type | `<entity>.<past-tense verb>` | `seat.reserved` |
| Message key | aggregate ID (keeps per-entity order) | seat ID |
| Consumer group | `<service>.<purpose>` | `notification.booking-events` |
| Retry topics | `<topic>.<group>.retry.<n>` | `booking.seat.v1.notification.booking-events.retry.1` |
| Dead-letter topic | `<topic>.<group>.dlt` | `booking.seat.v1.notification.booking-events.dlt` |

- **Format:** JSON. Every message value is the envelope in
  `contracts/envelope.schema.json`:
  `event_id, event_type, event_version, occurred_at, producer, aggregate_type,
  aggregate_id, company_id, vertical_id, tenant_id, correlation_id, actor,
  payload`.
- **Payload schemas** live in `contracts/<service>/<event_type>.v<N>.schema.json`.
  Only add optional fields. A breaking change is a new `event_version`,
  published alongside the old one until every reader has moved. Readers ignore
  unknown fields and unknown event types.
- **Never** put passwords, tokens, OTPs or secrets in events (one documented
  exception: the dedicated OTP topic, which skips the outbox).
- **Publishing:** business events go through the **outbox**, written in the same
  transaction as the data change. Publish directly with `kafka.Producer` only
  when losing the event on a crash is acceptable or it must not be stored.
- **Consuming:** manual commit after the work is durable. Handlers are
  idempotent: claim `event_id` in the inbox in the same transaction as the work.
- **Failures:** return an error to retry (5s, 30s, 2m, 10m by default), or
  `kafka.Permanent(err)` for bad data → dead-letter topic right away. Use
  `RetryInPlace` when strict per-key ordering matters more than throughput or
  when the handler owns retries itself.
- **Topics are never auto-created** at runtime. List the topics a service owns
  in its `internal/events` package so DevOps can create them; locally
  `make run svc=<svc> mode=topics` creates them. Producers use `acks=all`.

## HTTP

- Routes are versioned: `/v1/...`. Resource names are plural nouns.
- Responses: `{"data": ...}`, lists `{"data": [...], "meta": {"page","limit","total"}}`.
- Errors: `{"error": {"code","message","details","request_id"}}` with codes from
  `platform/apperror`. 5xx causes are logged, never returned.
- Pagination: `?page=1&limit=20` (max 100).
- Unsafe requests accept `Idempotency-Key` (Redis-backed replay for 24h).
- Health and metrics are on the admin port (8081), never through the ingress.

### Standard headers

| Header | Meaning |
|---|---|
| `Authorization: Bearer <jwt>` | access token from the identity provider |
| `X-Company-ID` | current workspace (company) — required on company routes |
| `X-Vertical-ID`, `X-Tenant-ID` | optional narrowing inside the company |
| `X-Request-ID` | per hop; generated if missing |
| `X-Correlation-ID` | whole user action across services and events |
| `X-On-Behalf-Of` | end-user ID on service-token calls |
| `Idempotency-Key` | client retry key for POST/PUT/PATCH/DELETE |

## Auth

- Every service verifies JWTs itself against the identity provider's JWKS
  (`AUTH_JWKS_URL`, `AUTH_ISSUER`, `AUTH_AUDIENCE` = this API).
- Tokens carry no company or roles. Permission checks go through the
  `auth.Authorizer` interface (implemented by the IAM client). Checks **fail
  closed**: if IAM can't answer, the request fails.
- Route check (`auth.RequirePermission`) uses the request's scope headers; for
  single records also check the record's own vertical/tenant (see
  `authorizeRecord` in the generated API) and answer 404, not 403.
- Lists use `auth.LoadAccessList` and filter with `entx.VisibleTo` /
  `mongo.VisibleTo`.
- Service-to-service calls use the client-credentials grant (one OAuth client
  per service).
- `AUTH_ENABLED=false` works only locally; `AUTH_AUTHORIZER=allow-all` is refused
  in production.

## Data

- **Scoping:** company-owned records carry `company_id` (+ optional
  `vertical_id`, `tenant_id`). In Postgres `entx.ScopeMixin` adds the filter to
  every query, update and delete automatically and fails if the context has no
  company. In Mongo use `mongo.Scoped`. System jobs opt out explicitly with
  `reqctx.WithoutScopeFilter`.
- **IDs:** UUIDv7 generated in Go.
- **No deletes:** business records change status.
- **Timestamps:** `created_at`, `updated_at` everywhere; UTC.
- **Postgres migrations:** Atlas versioned migrations generated from the ent
  schema (`make migrate-diff`) and reviewed in PRs. Never edit an applied
  migration. Never use ent auto-migrate.
- **Mongo:** indexes are created at startup by the store (`EnsureIndexes`).
  Transactions require a replica set.
- **Redis:** short-lived data only (cache, locks, counters, idempotency). Keys
  are prefixed `<service>:` and include the company ID for company data. Never
  the only copy of anything.

## Logging and observability

- `slog` JSON to stdout; the cluster ships it to Graylog. Always log with a
  context (`slog.InfoContext(ctx, ...)`) so request/correlation/company IDs are
  attached.
- Sensitive keys are redacted automatically (`password`, `token`, `otp`, …);
  add more with `LOG_REDACT_KEYS`. Don't log request bodies.
- Metrics: Prometheus on `:8081/metrics` (HTTP, Kafka consumer/producer,
  outbox backlog, DB pool).
- Tracing: OpenTelemetry, propagated over HTTP and Kafka headers; export is off
  until `OTEL_TRACING_ENABLED=true` and an OTLP endpoint exist.

## Code

- Layers: `api` (HTTP) → `service` (business logic) → `Store` interfaces →
  `store/<db>`. `service` never imports Gin, ent or the Mongo driver.
- Pass `context.Context` everywhere; never store it.
- Errors: return `apperror` values from service/store; wrap causes.
- Tests: service logic with fakes (see generated `item_test.go`); integration
  tests use the `integration` build tag.
- Generated code (ent) is committed; run `make gen` after schema changes.
