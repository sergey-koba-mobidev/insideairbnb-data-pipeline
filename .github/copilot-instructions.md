# GitHub Copilot Instructions

## Environment and Execution
- This project uses Docker for development and execution.
- When running commands related to the application (e.g., `pip`, `dagster`, `dbt`, `python`), prefer using `docker compose` to ensure commands run within the correct environment.
- For example, instead of running `pip install`, it may be necessary to run `docker compose exec <service-name> pip install` or rebuild the image if changes are made to `requirements.txt`.
- The main application logic resides in the `dagster_project/` directory.

## Linting and Formatting
- This project uses `ruff` for Python linting and formatting. Config is in `pyproject.toml`.
- **Mandatory**: After performing any file edits or code changes, you must immediately run `make format` followed by `make lint`.
- If linting errors are found:
    1. Run `make lint-fix` as a first attempt to resolve them.
    2. If issues persist, analyze errors and perform necessary manual edits.
    3. Repeat until `make lint` passes.
- Never consider a task "complete" until the codebase passes all linting checks.
