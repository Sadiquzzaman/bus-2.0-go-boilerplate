# Architecture

## One binary, several run modes

Each service builds one binary and one image. The first argument selects what
it runs, so each mode can be deployed and scaled on its own:

| Mode | Runs | Scales by |
|---|---|---|
| `api` | Gin HTTP server | CPU |
| `relay` | outbox → Kafka publisher | 2 replicas: one leader, one standby |
| `consumer` | Kafka consumer group(s) | up to the partition count |
| `worker` | scheduled/background jobs (add when needed) | — |
| `all` | everything, one process | local development only |

Every mode starts the admin server on `:8081` with `/livez`, `/readyz` and
`/metrics`. Dependencies are created lazily, so a mode only connects to what it
uses.

## A request

```
client ─▶ edge gateway (TLS, rate limit) ─▶ api pod
  recovery → request ID → tracing → metrics → access log → timeout → body limit
  → /v1 group: Authenticate (JWT via JWKS) → Scope (X-Company-ID …) → Idempotency-Key
  → route: RequirePermission / LoadAccessList (IAM Authorizer, fail closed)
  → handler → service → Store.Tx(...)
                         ├─ repository writes (company_id enforced by ScopeMixin)
                         └─ outbox row (same transaction)
```

## An event

```
service tx commit ──▶ outbox_events row
relay (leader via pg advisory lock / mongo lease)
  └─ reads unpublished rows in order → Kafka (acks=all, idempotent) → marks published
consumer group (manual commit)
  └─ decode envelope → restore scope, actor, correlation ID, trace
     → handler: Store.Tx { inbox.Claim(event_id) → work }
        ok        → commit offset
        error     → <topic>.<group>.retry.N (5s, 30s, 2m, 10m) → commit
        Permanent → <topic>.<group>.dlt → commit
        can't produce to retry/DLT → stop and retry (never skip)
```

Delivery is at-least-once end to end. The inbox claim makes handlers
effectively exactly-once for their own database.

## Shutdown

On SIGTERM: `/readyz` fails → wait `APP_SHUTDOWN_DELAY_SECONDS` (Kubernetes
removes the pod from endpoints) → cancel the mode (HTTP drains in-flight
requests; consumers stop polling and leave unfinished records uncommitted for
redelivery; the relay releases leadership) → close clients in reverse order.
`terminationGracePeriodSeconds` (40s) must exceed delay + timeout.

## Scoping model

Company → vertical → tenant, as defined by IAM. Tokens carry identity only.
The workspace arrives in headers; IAM answers "may this user do X here" and
"which verticals/tenants can this user see". Data layers enforce the company
wall automatically; vertical/tenant visibility is applied per query from the
access list, and single-record reads re-check the record's own scope.

## What is deliberately left to services

- Business entities, permissions names, and the topics they own.
- The IAM `Authorizer` implementation (plug in when the IAM Go library exists —
  `platform/auth/authorizer.go`).
- Workers/schedulers (add a `worker` mode when a service needs one).
