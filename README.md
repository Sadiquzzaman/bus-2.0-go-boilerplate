# bus-2.0 Go boilerplate

Monorepo foundation for the bus-2.0 microservices: a shared **platform kit**,
a **service generator** and local infrastructure for development.
Build, CI/CD and deployment are owned by DevOps and are not part of this repo.

Stack: Go 1.26 · Gin · ent (PostgreSQL) or the official MongoDB driver ·
Kafka (franz-go) · Redis · OpenTelemetry · Prometheus · logs to Graylog.

```
platform/            shared kit — imported by every service, no business logic
  app/               run modes, admin server (/livez /readyz /metrics), graceful shutdown
  config/            env-based config, fail fast
  logger/            slog JSON → stdout → Graylog, context fields, redaction
  reqctx/            request/correlation IDs, company→vertical→tenant scope, actor
  apperror/          one error type, one JSON error shape
  httpserver/        Gin server, middleware, responses, pagination, scope headers
  auth/              JWT (JWKS) authentication, Authorizer interface for IAM
  httpclient/        service-to-service GETs: retries, breaker, token, propagation
  idempotency/       Idempotency-Key middleware (Redis)
  postgres/ + entx/  pgx pool for ent, UUIDv7/time/scope mixins, outbox/inbox schemas
  mongo/             client, transactions, scope helpers
  redis/             client, JSON cache, locks, rate limiter
  kafka/             envelope, producer, consumer (manual commit, retry topics, DLT)
  outbox/            relay + pgoutbox / mongooutbox
  inbox/             pginbox / mongoinbox (idempotent consumers)
  observability/     OpenTelemetry tracing (off by default), Prometheus
services/
  example-pg/        Postgres + Kafka + Redis sample (generated)
  example-mongo/     Mongo + Kafka sample consuming example-pg's events (generated)
tools/scaffold/      the service generator
contracts/           event envelope JSON Schema (+ per-service payload schemas)
docker-compose.yml   local postgres, mongo (replica set), kafka, kafka-ui, redis
scripts/             bootstrap script, local postgres init
docs/                conventions and architecture notes
```

## Quick start

Requirements: Go 1.26+, Docker, `make`. Optional: [Atlas](https://atlasgo.io) for migrations, `golangci-lint`.

```bash
make bootstrap          # first time: fetch deps, ent codegen, tidy, build, test
make up                 # local postgres, mongo, kafka, redis

cp services/example-pg/.env.example    services/example-pg/.env
cp services/example-mongo/.env.example services/example-mongo/.env

make migrate-diff svc=example-pg name=init   # if bootstrap didn't (needs atlas + docker)
make migrate-up   svc=example-pg

make run svc=example-pg    mode=topics
make run svc=example-mongo mode=topics
make run svc=example-pg    mode=all     # terminal 1: API :8080, relay, consumer
make run svc=example-mongo mode=all     # terminal 2: API :8090, relay, consumer
```

Create an item in example-pg; watch the event flow through the outbox into
Kafka (Kafka UI at http://localhost:8085) and get consumed by example-mongo:

```bash
curl -s -X POST localhost:8080/v1/items \
  -H "X-Company-ID: 0190a8f0-0000-7000-8000-000000000001" \
  -H "Content-Type: application/json" -H "Idempotency-Key: $(uuidgen)" \
  -d '{"name":"Dhaka → Chattogram"}'
```

## New service

```bash
make new-service name=booking db=postgres kafka=true redis=true
make new-service name=notification db=mongo kafka=true redis=false
make new-service name=gateway db=none kafka=false redis=true
```

The generated service contains only what it uses; everything shared comes from
`platform/`. A Mongo service never compiles ent or pgx into its binary, and a
Postgres service never compiles the Mongo driver.

Every service has the same shape:

```
cmd/<svc>/main.go        run modes: api | relay | consumer | topics | all
internal/config          env config (platform configs + service settings)
internal/bootstrap       lazy wiring per run mode
internal/api             Gin handlers + permission checks
internal/domain          business types
internal/service         business logic behind Store interfaces (unit-tested with fakes)
internal/store/<db>      persistence
internal/events          owned topics, event types, payloads
internal/consumer        Kafka handlers (idempotent via inbox)
ent/schema, migrations   (Postgres only)
```

## Read next

- [docs/conventions.md](docs/conventions.md) — the rules every service follows (topics, events, headers, errors, scoping, retries, migrations).
- [docs/architecture.md](docs/architecture.md) — how requests, the outbox, consumers and shutdown work.

## Make targets

`make help` lists everything. The common ones:

| Target | |
|---|---|
| `make bootstrap` | deps + codegen + build + test |
| `make up` / `make down` | local infra |
| `make new-service name=… db=…` | scaffold a service |
| `make gen` | ent codegen for all services |
| `make run svc=… mode=…` | run a service locally |
| `make test` / `make lint` | tests / golangci-lint |
| `make db svc=…` | create a local Postgres database |
| `make migrate-diff svc=… name=…` / `make migrate-up svc=…` | Atlas migrations |
