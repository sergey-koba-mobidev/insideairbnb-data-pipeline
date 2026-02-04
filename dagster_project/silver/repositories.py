import os
import random
import time
import duckdb
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import (
    CommitFailedException,
    NamespaceAlreadyExistsError,
    NoSuchTableError,
    TableAlreadyExistsError,
)
from typing import Any, List
import pyarrow as pa


class NessieCatalogRepository:
    def __init__(self, context_log: Any):
        self.log = context_log
        self.con = duckdb.connect()
        self._setup_duckdb()
        self.catalog = self._setup_catalog()

    def _setup_duckdb(self):
        self.log.info("Installing/Loading DuckDB extensions...")
        self.con.execute(
            "INSTALL iceberg; INSTALL httpfs; INSTALL spatial; LOAD iceberg; LOAD httpfs; LOAD spatial;"
        )

        self.log.info("Configuring DuckDB S3 secret...")
        self.con.execute(
            f"""
            CREATE OR REPLACE SECRET minio_s3 (
                TYPE S3,
                KEY_ID '{os.getenv("AWS_ACCESS_KEY_ID")}',
                SECRET '{os.getenv("AWS_SECRET_ACCESS_KEY")}',
                ENDPOINT '{os.getenv("S3_ENDPOINT", "minio:9000").replace("http://", "")}',
                URL_STYLE 'path',
                USE_SSL false
            );
        """
        )

    def _setup_catalog(self):
        self.log.info("Initializing PyIceberg catalog...")
        return load_catalog(
            "nessie",
            **{
                "type": "rest",
                "uri": os.getenv("NESSIE_URI", "http://nessie:19120/iceberg"),
                "s3.endpoint": os.getenv("S3_ENDPOINT", "http://minio:9000"),
                "s3.access-key-id": os.getenv("AWS_ACCESS_KEY_ID"),
                "s3.secret-access-key": os.getenv("AWS_SECRET_ACCESS_KEY"),
                "s3.region": os.getenv("AWS_REGION", "us-east-1"),
                "s3.path-style-access": "true",
            },
        )

    def ensure_namespace(self, namespace: str):
        try:
            self.catalog.create_namespace(namespace)
            self.log.info(f"Namespace '{namespace}' created.")
        except NamespaceAlreadyExistsError:
            self.log.info(f"Namespace '{namespace}' already exists.")

    def get_columns_in_file(self, s3_source: str) -> List[str]:
        try:
            return [
                row[0]
                for row in self.con.execute(
                    f"DESCRIBE SELECT * FROM read_csv_auto('{s3_source}', ignore_errors=true)"
                ).fetchall()
            ]
        except duckdb.Error as e:
            self.log.warning(f"Could not describe {s3_source}: {e}")
            return []

    def fetch_arrow_table(self, query: str) -> pa.Table:
        return self.con.execute(query).fetch_arrow_table()

    def append_to_iceberg(self, identifier: str, arrow_table: pa.Table):
        max_retries = 10
        for attempt in range(max_retries):
            try:
                try:
                    table = self.catalog.load_table(identifier)
                    self.log.info(f"Loaded existing table {identifier}")
                except NoSuchTableError:
                    self.log.info(f"Creating new Iceberg table {identifier}...")
                    try:
                        table = self.catalog.create_table(
                            identifier,
                            schema=arrow_table.schema,
                        )
                    except TableAlreadyExistsError:
                        self.log.info(
                            f"Table {identifier} was created concurrently. Loading..."
                        )
                        table = self.catalog.load_table(identifier)

                # Ensure name mapping exists for better compatibility with non-Iceberg sources
                if "schema.name-mapping.default" not in table.properties:
                    from pyiceberg.table.name_mapping import create_mapping_from_schema

                    with table.transaction() as tx:
                        tx.set_properties(
                            {
                                "schema.name-mapping.default": create_mapping_from_schema(
                                    table.schema()
                                ).model_dump_json()
                            }
                        )
                    table = self.catalog.load_table(identifier)

                # Evolve schema if necessary using union_by_name
                # union_by_name supports pyarrow.Schema and handles name-based mapping
                self.log.info(f"Syncing schema for {identifier}...")
                with table.update_schema() as update:
                    update.union_by_name(arrow_table.schema)

                # Reload table after potential schema update
                table = self.catalog.load_table(identifier)

                self.log.info(
                    f"Appending data to {identifier} (attempt {attempt + 1})..."
                )
                table.append(arrow_table)
                self.log.info(f"Appended {arrow_table.num_rows} rows to {identifier}")
                return

            except CommitFailedException as e:
                if attempt < max_retries - 1:
                    # Exponential backoff with jitter
                    wait_time = (2**attempt) + (random.randint(0, 1000) / 1000.0)
                    self.log.warning(
                        f"Commit failed for {identifier} (concurrent update). "
                        f"Retrying in {wait_time:.2f}s... Error: {e}"
                    )
                    time.sleep(wait_time)
                else:
                    self.log.error(f"Max retries reached for {identifier}. Failing.")
                    raise e
            except Exception as e:
                self.log.error(f"Unexpected error appending to {identifier}: {e}")
                raise e
