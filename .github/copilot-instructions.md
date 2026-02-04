# GitHub Copilot Instructions

## Project Architecture
This project is an ELT pipeline for Inside Airbnb data using the "Medallion Architecture" (Bronze/Silver/Gold):

### 1. The Ingestion Layer (Bronze) -> Python/MinIO
- **Location**: `dagster_project/`
- **Orchestration**: Dagster Software-Defined Assets (SDA) with Dynamic Partitions (`City|Country|Date`).
- **Scraping**: `BeautifulSoup4` & `requests` (using browser-like headers to avoid 403s).
- **Storage**: Raw CSV and GeoJSON files stored in MinIO (S3-compatible).
- **Automation**: `airbnb_data_monitor_sensor` scans the website every 5 minutes, creates partitions, and triggers runs if files are missing.
- **Data Integrity**: S3 paths are normalized (ASCII only) using `unicodedata` to ensure cross-system compatibility.

## Environment and Execution
- This project uses Docker for development and execution.
- When running commands related to the application (e.g., `pip`, `dagster`, `dbt`, `python`), prefer using `docker compose` to ensure commands run within the correct environment.
- For example, instead of running `pip install`, it may be necessary to run `docker compose exec <service-name> pip install` or rebuild the image if changes are made to `requirements.txt`.
- The main application logic resides in the `dagster_project/` directory.
- After any changes to the Dagster code, validate the project using `make dagster-validate` to ensure there are no issues.

## Linting and Formatting
- This project uses `ruff` for Python linting and formatting. Config is in `pyproject.toml`.
- **Mandatory**: After performing any file edits or code changes, you must immediately run `make format` followed by `make lint`.
- If linting errors are found:
    1. Run `make lint-fix` as a first attempt to resolve them.
    2. If issues persist, analyze errors and perform necessary manual edits.
    3. Repeat until `make lint` passes.
- Never consider a task "complete" until the codebase passes all linting checks.
