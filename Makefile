# Day-to-day commands. On Windows without make, run the commands below directly
# (they are also listed in README.md).
COMPOSE := docker compose -f deploy/compose/compose.yaml

.PHONY: generate lint-proto build test up down logs smoke

generate:   ## Generate Go code from the protos (needs buf)
	buf generate

lint-proto: ## Lint the Protobuf contracts
	buf lint proto

build:      ## Build every service
	go build ./...

test:       ## Unit tests
	go vet ./...
	go test ./...

up:         ## Start the local environment
	$(COMPOSE) up --build -d

down:       ## Stop the local environment (data stays in the volumes)
	$(COMPOSE) down

logs:       ## Follow the logs of every service
	$(COMPOSE) logs -f --tail=100

smoke:      ## End-to-end test: from a post to its comments
	bash scripts/smoke.sh
