import os
import duckdb
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError, NoSuchTableError
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
                    f"DESCRIBE SELECT * FROM read_csv_auto('{s3_source}')"
                ).fetchall()
            ]
        except duckdb.Error as e:
            self.log.warning(f"Could not describe {s3_source}: {e}")
            return []

    def fetch_arrow_table(self, query: str) -> pa.Table:
        return self.con.execute(query).fetch_arrow_table()

    def append_to_iceberg(self, identifier: str, arrow_table: pa.Table):
        try:
            table = self.catalog.load_table(identifier)
            self.log.info(f"Loaded existing table {identifier}")
        except NoSuchTableError:
            self.log.info(f"Creating new Iceberg table {identifier}...")
            table = self.catalog.create_table(
                identifier,
                schema=arrow_table.schema,
            )

        self.log.info(f"Appending data to {identifier}...")
        table.append(arrow_table)
        self.log.info(f"Appended {arrow_table.num_rows} rows to {identifier}")
