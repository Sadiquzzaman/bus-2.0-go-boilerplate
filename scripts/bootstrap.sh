#!/usr/bin/env bash
# First-time setup: resolve dependencies, run ent codegen, tidy, build, test.
# Safe to re-run. Commit go.mod, go.sum and generated ent code afterwards.
set -euo pipefail
cd "$(dirname "$0")/.."

need() { command -v "$1" >/dev/null 2>&1 || { echo "missing: $1 ($2)"; exit 1; }; }
need go "https://go.dev/dl — Go 1.26+"

gov=$(go env GOVERSION | sed 's/^go//')
major=${gov%%.*}; rest=${gov#*.}; minor=${rest%%.*}
if (( major < 1 || (major == 1 && minor < 26) )); then
  echo "Go 1.26+ required (found $gov). With GOTOOLCHAIN=auto Go downloads it for you."
fi

# Direct dependencies of platform/ and the service templates.
deps=(
  github.com/caarlos0/env/v11
  github.com/joho/godotenv
  github.com/gin-gonic/gin
  github.com/go-playground/validator/v10
  github.com/google/uuid
  github.com/prometheus/client_golang
  go.opentelemetry.io/otel
  go.opentelemetry.io/otel/sdk
  go.opentelemetry.io/otel/trace
  go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp
  go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin
  go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp
  github.com/MicahParks/keyfunc/v3
  github.com/golang-jwt/jwt/v5
  github.com/sony/gobreaker/v2
  golang.org/x/oauth2
  entgo.io/ent
  github.com/jackc/pgx/v5
  github.com/XSAM/otelsql
  go.mongodb.org/mongo-driver/v2
  github.com/redis/go-redis/v9
  github.com/twmb/franz-go
  github.com/twmb/franz-go/pkg/kadm
)

if ! grep -q "^require" go.mod; then
  echo "==> resolving dependencies (latest versions)"
  go get "${deps[@]/%/@latest}"
fi

echo "==> ent codegen"
go generate ./services/...

echo "==> go mod tidy"
go mod tidy

echo "==> build"
go build ./...

echo "==> unit tests"
go test -count=1 ./...

# Initial migrations for Postgres services that have none yet.
if command -v atlas >/dev/null 2>&1 && docker info >/dev/null 2>&1; then
  for dir in services/*/; do
    svc=$(basename "$dir")
    if [[ -f "$dir/atlas.hcl" ]] && ! ls "$dir/migrations"/*.sql >/dev/null 2>&1; then
      echo "==> initial migration for $svc"
      (cd "$dir" && atlas migrate diff init --env local)
    fi
  done
else
  echo "(skip) atlas or docker not available — run 'make migrate-diff svc=<svc> name=init' later"
fi

echo
echo "Done. Next: make up && make run svc=example-pg mode=all"
