# --- Variables ---
ENV_FILE := .env
ENV_EXAMPLE := .env.example

# --- Default Goal ---
.DEFAULT_GOAL := help

.PHONY: help setup build up down ps logs clean lint format

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

clean: ## Remove containers, networks, and volumes
	docker compose down -v
	@echo "Note: .env file was NOT removed. Delete it manually if needed."

lint: ## Run linter (ruff) on dagster_project
	docker compose exec dagster_daemon ruff check .

lint-fix: ## Run linter and fix issues automatically
	docker compose exec dagster_daemon ruff check --fix .

format: ## Run formatter (ruff) on dagster_project
	docker compose exec dagster_daemon ruff format .

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
