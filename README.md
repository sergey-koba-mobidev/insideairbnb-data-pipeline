# insideairbnb-data-pipeline
Modern data pipeline project for Analytics and Machine Learning.

## Getting Started
- `make setup`
- `make build`

## Data Pipeline
### Phase 1: Ingestion & Orchestration
Dagster or Prefect.
Dagster is better suited for an "Asset-based" approach. It doesn't just run tasks; it tracks the data assets (e.g., "The London Listings File"). It has built-in retries, beautiful logging, and a "Sensor" feature that can periodically check the Inside Airbnb URL for new monthly files.

### Phase 2: The Iceberg Layer
PyIceberg + DuckDB.
In 2026, spinning up a Spark Thrift Server for a single-person project is overkill ("The Spark Tax"). PyIceberg (v0.9.0+) now supports native writes. You can use DuckDB as the engine to read the CSV from MinIO and write it into the Iceberg format. It’s significantly faster and requires zero JVM/Java overhead.
Storage: MinIO (using the S3 API).
Iceberg Catalog: Use Project Nessie (running in a Docker container) as your catalog. It provides "Git-like" branching for your data.

### Phase 3: The Analytics Layer (ClickHouse + dbt)
dbt-clickhouse.
ClickHouse has a native Iceberg Storage Engine. You don't necessarily need to "move" the data in a heavy ETL sense.
Workflow: 1. Create an EXTERNAL TABLE in ClickHouse pointing to your Iceberg files in MinIO. 2. Use dbt to create Materialized Views inside ClickHouse. This will "pull" the data from the Iceberg "Cold" layer into ClickHouse's "Hot" (MergeTree) storage for sub-second dashboarding.

### Phase 4: Visualization (Python-Based & Self-Hosted)
Evidence.dev or Streamlit.
Evidence.dev: It is a "Business Intelligence as Code" tool. You write Markdown and SQL. It’s perfect for a developer because your entire dashboard can live in your GitHub repo alongside your dbt models.


## Endpoints

### Dagster Web UI
After running `make up`, access the Dagster UI at: [http://localhost:3000](http://localhost:3000)

### MinIO Web UI
After running `make up`, access the MinIO Console at: [http://localhost:9001](http://localhost:9001)

**Default credentials** (see your `.env` file or `.env.example`):
- Username: `minioadmin` (or `${MINIO_ROOT_USER}`)
- Password: `minioadmin` (or `${MINIO_ROOT_PASSWORD}`)

## Development
This project uses `ruff` for linting and formatting.

- `make lint`: Check for linting errors.
- `make lint-fix`: Automatically fix common linting errors.
- `make format`: Format code according to project style.

## Data Source & Attribution

This project uses data provided by [Inside Airbnb](https://insideairbnb.com/).

* **Source:** [Inside Airbnb - Get the Data](https://insideairbnb.com/get-the-data/)
* **License:** [Creative Commons Attribution 4.0 International (CC BY 4.0)](https://creativecommons.org/licenses/by/4.0/)
* **Disclaimer:** This project is an independent research work and is not associated with or endorsed by Inside Airbnb or Airbnb, Inc. The data is utilized for non-commercial, educational, and research purposes.

### Citation
If you use the findings or pipeline from this project, please cite the original data source:
> Cox, M. (2026). Inside Airbnb. Retrieved from http://insideairbnb.com/
