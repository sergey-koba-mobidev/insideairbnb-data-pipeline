# insideairbnb-data-pipeline
Modern data pipeline project for Analytics and Machine Learning using the Medallion Architecture (Bronze, Silver, Gold).

## Quick Start (Setup)

To get the pipeline running from scratch, follow these steps:

1.  **Environment Setup**:
    ```bash
    make setup
    ```
    *This creates a `.env` file from the example. You can edit it to change credentials.*

2.  **Start Infrastructure**:
    ```bash
    make build
    ```
    *Builds Docker images for Dagster (webserver & daemon), ClickHouse, MinIO, and Nessie.*

3.  **Run Containers**:
    ```bash
    make up
    ```

4.  **Initialize ClickHouse Connection**:
    ```bash
    make clickhouse-setup
    ```
    *This is a one-time mandatory step that establishes the connection between ClickHouse and the Nessie catalog.*

## Project Architecture
This project implements an ELT pipeline:

### 1. Ingestion Layer (Bronze) -> Python/MinIO
- **Scraping**: `BeautifulSoup4` & `requests` fetch the latest Airbnb datasets.
- **Orchestration**: Dagster sensors monitor the website every 5 minutes and trigger runs for new files.
- **Storage**: Raw CSV and GeoJSON files are stored in a MinIO S3 bucket (`airbnb-data/bronze`).

### 2. Processing Layer (Silver) -> Iceberg/Nessie
- **Format**: Data is converted from CSV to **Apache Iceberg** tables.
- **Engine**: DuckDB handles the heavy lifting of reading from S3 and writing Iceberg metadata.
- **Catalog**: **Project Nessie** acts as the catalog, providing Git-like versioning (branching/merging) for your data lake.

### 3. Analytics Layer (Gold) -> ClickHouse/dbt
- **Direct Access**: ClickHouse queries the Silver Iceberg tables **directly** through a `DataLakeCatalog` engine. There is no redundant data copying.
- **Transformation**: `dbt` runs on top of ClickHouse to create clean, modeled "Marts" for analytics.

## Endpoints
- **Dagster UI**: [http://localhost:3000](http://localhost:3000)
- **MinIO Console**: [http://localhost:9001](http://localhost:9001)
- **Nessie UI**: [http://localhost:19120](http://localhost:19120)
- **ClickHouse**: `localhost:8123` (HTTP) / `9006` (Native)

## Maintenance & Tools
- **Resetting everything**: `make wipe-all` (Deletes Dagster history, Nessie catalog, and S3 data).
- **Restarting services**: `make restart`.
- **Wiping just metadata**: `make nessie-wipe` (Clears RocksDB storage).
- **Formatting/Linting**: `make format` and `make lint` are mandatory before committing code.

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
