# --- Variables ---
ENV_FILE := .env
ENV_EXAMPLE := .env.example

# --- Default Goal ---
.DEFAULT_GOAL := help

.PHONY: help setup build up down ps logs clean lint format restart

help: ## Show this help message
	@echo "Usage: make [target]"
	@echo ""
	@echo "Targets:"
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

setup: ## Copy .env.example to .env if it doesn't exist
	@if [ ! -f $(ENV_FILE) ]; then \
		echo "Copying $(ENV_EXAMPLE) to $(ENV_FILE)..."; \
		cp $(ENV_EXAMPLE) $(ENV_FILE); \
		echo "Done. Please edit $(ENV_FILE) with your specific credentials."; \
	else \
		echo "$(ENV_FILE) already exists. Skipping copy."; \
	fi

build: setup ## Build or rebuild docker images
	docker compose build

up: setup ## Start the infrastructure in the background
	docker compose up -d

down: ## Stop and remove containers
	docker compose down

ps: ## Check status of containers
	docker compose ps

logs: ## Tail logs for all services
	docker compose logs -f

clean: ## Remove contmakeainers, networks, and volumes
	docker compose down -v
	@echo "Note: .env file was NOT removed. Delete it manually if needed."

dagster-validate: ## Validate Dagster project
	docker compose run --rm --entrypoint dagster dagster_daemon definitions validate -f definitions.py

dagster-wipe: ## Wipe Dagster instance
	docker compose run --rm --entrypoint dagster dagster_webserver run wipe
	docker compose run --rm --entrypoint dagster dagster_webserver asset wipe --all

nessie-wipe: ## Wipe Nessie catalog (RocksDB data)
	docker compose stop nessie
	docker compose run --rm --entrypoint sh nessie -c "rm -rf /data/nessie/*"
	docker compose start nessie

minio-wipe: ## Wipe all data in MinIO (airbnb-data bucket)
	docker compose exec minio sh -c "rm -rf /data/airbnb-data/*"

clickhouse-setup: ## Run ClickHouse Iceberg setup script
	docker compose exec dagster_daemon python setup_clickhouse.py

wipe-all: dagster-wipe nessie-wipe minio-wipe ## Wipe all data (Dagster, Nessie, and MinIO)

lint: ## Run linter (ruff) on dagster_project
	docker compose run --rm --entrypoint ruff dagster_daemon check .

lint-fix: ## Run linter and fix issues automatically
	docker compose run --rm --entrypoint ruff dagster_daemon check --fix .

format: ## Run formatter (ruff) on dagster_project
	docker compose run --rm --entrypoint ruff dagster_daemon format .

export-code: ## Export all Python code and YAMLs into code_listing.md
	@echo "Exporting code to code_listing.md..."
	@echo "# Project Code Listing" > code_listing.md
	@echo "" >> code_listing.md
	@echo "# .github/copilot-instructions.md" >> code_listing.md
	@echo "\`\`\`markdown" >> code_listing.md
	@cat .github/copilot-instructions.md >> code_listing.md
	@echo "" >> code_listing.md
	@echo "\`\`\`" >> code_listing.md
	@echo "" >> code_listing.md
	@find . -type f \( -name "*.py" -o -name "*.yaml" -o -name "*.yml" \) \
		-not -path "*/.*" \
		-not -path "*/__pycache__/*" \
		-not -path "*/node_modules/*" \
		-not -path "*/venv/*" | while read -r file; do \
			echo "# $$file" >> code_listing.md; \
			echo "\`\`\`" >> code_listing.md; \
			cat "$$file" >> code_listing.md; \
			echo "" >> code_listing.md; \
			echo "\`\`\`" >> code_listing.md; \
			echo "" >> code_listing.md; \
		done
	@echo "Done! File created at code_listing.md"

restart: ## Stop and restart all containers
	docker compose stop
	docker compose up -d
