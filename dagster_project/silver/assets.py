import os
import yaml
from dagster import (
    AssetExecutionContext,
    multi_asset,
    AssetOut,
    RetryPolicy,
    Backoff,
)
from resources import MinIOResource
from partitions import airbnb_partitions
from shared.utils import normalize_string, repair_mangled_string
from silver.repositories import NessieCatalogRepository
from bronze.assets import airbnb_raw_data


@multi_asset(
    outs={
        "silver_listings": AssetOut(group_name="silver"),
        "silver_reviews": AssetOut(group_name="silver"),
        "silver_neighbourhoods": AssetOut(group_name="silver"),
        "silver_geo_neighbourhoods": AssetOut(group_name="silver"),
    },
    deps=[airbnb_raw_data],
    partitions_def=airbnb_partitions,
    compute_kind="duckdb",
    retry_policy=RetryPolicy(
        max_retries=3,
        delay=60,  # Wait 1 minute
        backoff=Backoff.EXPONENTIAL,  # Wait longer each time
    ),
)
def airbnb_iceberg_tables(context: AssetExecutionContext, minio: MinIOResource):
    partition_key = context.partition_key
    parts = partition_key.split("|")
    if len(parts) != 3:
        raise ValueError(
            f"Partition key {partition_key} is not in the format 'city|country|date'"
        )

    city, country, date = parts

    city = repair_mangled_string(city)
    country = repair_mangled_string(country)

    prefix = f"{normalize_string(country)}/{normalize_string(city)}/{date}/"
    bucket = os.getenv("MINIO_BUCKET_NAME", "airbnb-data")

    repo = NessieCatalogRepository(context.log)
    repo.ensure_namespace("silver")

    # Load overrides
    config_path = os.path.join(
        os.path.dirname(__file__), "..", "config", "column_overrides.yaml"
    )
    with open(config_path, "r") as f:
        all_overrides = yaml.safe_load(f)

    tables = [
        ("listings", "listings.csv", False),
        ("reviews", "reviews.csv", False),
        ("neighbourhoods", "neighbourhoods.csv", False),
        ("geo_neighbourhoods", "neighbourhoods.geojson", True),
    ]

    for table_name, file_name, is_spatial in tables:
        s3_source = f"s3://{bucket}/{prefix}{file_name}"
        identifier = f"silver.{table_name}"

        type_overrides = all_overrides.get(table_name, {})

        if is_spatial:
            load_query = f"""
                SELECT
                    * EXCLUDE (geometry),
                    geometry::VARCHAR as geometry,
                    '{country}'::VARCHAR as partition_country,
                    '{city}'::VARCHAR as partition_city,
                    '{date}'::DATE as partition_date
                FROM ST_Read('{s3_source}')
            """
        else:
            columns_in_file = repo.get_columns_in_file(s3_source)
            filtered_overrides = {
                k: v for k, v in type_overrides.items() if k in columns_in_file
            }

            types_str = ""
            if filtered_overrides:
                hints = ", ".join(
                    [f"'{k}': '{v}'" for k, v in filtered_overrides.items()]
                )
                types_str = f", types={{{hints}}}"

            load_query = f"""
                SELECT
                    *,
                    '{country}'::VARCHAR as partition_country,
                    '{city}'::VARCHAR as partition_city,
                    '{date}'::DATE as partition_date
                FROM read_csv_auto('{s3_source}'{types_str}, ignore_errors=true)
            """

        arrow_table = repo.fetch_arrow_table(load_query)
        repo.append_to_iceberg(identifier, arrow_table)

    return None, None, None, None
