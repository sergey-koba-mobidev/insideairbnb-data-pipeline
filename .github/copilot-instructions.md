# GitHub Copilot Instructions

## Environment and Execution
- This project uses Docker for development and execution.
- When running commands related to the application (e.g., `pip`, `dagster`, `dbt`, `python`), prefer using `docker compose` to ensure commands run within the correct environment.
- For example, instead of running `pip install`, it may be necessary to run `docker compose exec <service-name> pip install` or rebuild the image if changes are made to `requirements.txt`.
- The main application logic resides in the `dagster_project/` directory.
