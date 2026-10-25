# bus-2.0 Go monorepo. Run `make help`.

SHELL      := /bin/bash
SERVICES   := $(notdir $(wildcard services/*))
svc        ?=
mode       ?= all
name       ?=
db         ?= postgres
kafka      ?= true
redis      ?= true
COMPOSE    := docker compose

.DEFAULT_GOAL := help

## help: list targets
help:
	@grep -E '^## ' $(MAKEFILE_LIST) | sed 's/## /  /' | column -t -s ':'

## bootstrap: first-time setup — fetch deps, run codegen, tidy
bootstrap:
	@bash scripts/bootstrap.sh

## up: start local infra (postgres, mongo, kafka, redis, kafka-ui)
up:
	$(COMPOSE) up -d --wait

## down: stop local infra (keeps volumes)
down:
	$(COMPOSE) down

## nuke: stop local infra and delete volumes
nuke:
	$(COMPOSE) down -v

## new-service: scaffold a service — make new-service name=booking db=postgres|mongo|none kafka=true redis=true
new-service:
	@test -n "$(name)" || (echo "usage: make new-service name=<svc> [db=postgres|mongo|none] [kafka=true] [redis=true]" && exit 1)
	go run ./tools/scaffold -name $(name) -db $(db) -kafka=$(kafka) -redis=$(redis)
	@if [ "$(db)" = "postgres" ]; then $(MAKE) gen; fi
	go mod tidy

## gen: run go generate (ent codegen) for all services
gen:
	go generate ./services/...

## run: run a service locally — make run svc=example-pg mode=all|api|relay|consumer|topics
run: _need-svc
	cd services/$(svc) && go run ./cmd/$(svc) $(mode)

## build: build all service binaries into bin/
build:
	@mkdir -p bin
	@for s in $(SERVICES); do echo "build $$s"; CGO_ENABLED=0 go build -trimpath -o bin/$$s ./services/$$s/cmd/$$s || exit 1; done

## test: unit tests
test:
	go test -race -count=1 ./...

## test-integration: integration tests (need Docker; build tag "integration")
test-integration:
	go test -race -count=1 -tags integration ./...

## lint: golangci-lint
lint:
	golangci-lint run ./...

## fmt: gofmt + goimports
fmt:
	gofmt -s -w platform services tools
	@command -v goimports >/dev/null && goimports -local github.com/Sadiquzzaman/bus-2.0-go-boilerplate -w platform services tools || true

## db: create the local postgres database for a service — make db svc=example-pg
db: _need-svc
	$(COMPOSE) exec -T postgres sh -c 'psql -U postgres -tc "SELECT 1 FROM pg_database WHERE datname = '\''$(subst -,_,$(svc))'\''" | grep -q 1 || createdb -U postgres $(subst -,_,$(svc))'

## migrate-diff: generate a migration from the ent schema — make migrate-diff svc=example-pg name=add_x
migrate-diff: _need-svc
	@test -n "$(name)" || (echo "name= is required" && exit 1)
	cd services/$(svc) && atlas migrate diff $(name) --env local

## migrate-up: apply migrations to the local DB — make migrate-up svc=example-pg
migrate-up: _need-svc
	cd services/$(svc) && set -a && source .env && set +a && atlas migrate apply --env local

## migrate-status: show migration status — make migrate-status svc=example-pg
migrate-status: _need-svc
	cd services/$(svc) && set -a && source .env && set +a && atlas migrate status --env local

_need-svc:
	@test -n "$(svc)" || (echo "svc= is required, one of: $(SERVICES)" && exit 1)
	@test -d services/$(svc) || (echo "unknown service $(svc)" && exit 1)

.PHONY: help bootstrap up down nuke new-service gen run build test test-integration lint fmt db migrate-diff migrate-up migrate-status _need-svc
