.PHONY: help build test test-race test-integration vet tidy up down logs

help: ## List commands
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

build: ## Build api and worker binaries to ./bin
	go build -o bin/api ./cmd/api
	go build -o bin/worker ./cmd/worker

test: ## Unit tests
	go test ./...

test-race: ## Tests with race detector (requires CGO/gcc)
	CGO_ENABLED=1 go test -race ./...

test-integration: ## Integration tests (requires TEST_POSTGRES_DSN)
	go test -tags=integration ./...

vet: ## Static analysis
	go vet ./...

tidy: ## go mod tidy
	go mod tidy

up: ## Start full system (migrations applied automatically)
	docker compose up --build

down: ## Stop and remove containers and volumes
	docker compose down -v

logs: ## Tail api and worker logs
	docker compose logs -f api worker
