COMPOSE ?= docker compose
SERVICE_API := documents-api

.PHONY: help up down restart build logs ps seed process bench-api test test-laravel test-python stats dlq clean

help: ## Show available targets
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

up: ## Build and start the whole stack
	$(COMPOSE) up -d --build

down: ## Stop the stack (keeps volumes)
	$(COMPOSE) down

restart: ## Recreate the application containers
	$(COMPOSE) up -d --build --force-recreate laravel laravel-worker documents-api documents-worker

build: ## Build all images
	$(COMPOSE) build

logs: ## Follow logs
	$(COMPOSE) logs -f --tail=100

ps: ## Show container status
	$(COMPOSE) ps

seed: ## Recreate the 100k invoice dataset
	$(COMPOSE) exec laravel php -d memory_limit=256M artisan invoices:seed --count=100000 --chunk=500 --fresh

process: ## Process pending invoices under a hard 32 MB PHP limit
	$(COMPOSE) exec laravel php -d memory_limit=32M artisan invoices:process --json

bench-api: ## Measure POST /documents/process latency (1000 docs x 20)
	$(COMPOSE) exec $(SERVICE_API) python scripts/bench_endpoint.py --url http://127.0.0.1:8000 --documents 1000 --iterations 20

stats: ## Print FastAPI runtime statistics
	@curl -fsS http://127.0.0.1:$${DOCUMENTS_API_PORT:-8001}/api/v1/documents/stats | python3 -m json.tool

dlq: ## Show dead-letter queue depth
	$(COMPOSE) exec redis redis-cli XLEN documents:dlq

test: test-laravel test-python ## Run all test suites

test-laravel: ## Run PHPUnit inside the testing image
	$(COMPOSE) run --rm --build laravel-test

test-python: ## Run pytest inside the service image
	$(COMPOSE) run --rm --build $(SERVICE_API) pytest

clean: ## Stop the stack and delete volumes (data loss)
	$(COMPOSE) down -v
