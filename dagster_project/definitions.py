import os
from dagster import Definitions, load_assets_from_modules, define_asset_job
from bronze import assets as bronze_assets
from bronze import sensors as bronze_sensors
from silver import assets as silver_assets
from silver import sensors as silver_sensors
from resources import MinIOResource
from partitions import airbnb_partitions

# Load assets
all_assets = load_assets_from_modules([bronze_assets, silver_assets])

# Job to materialize the airbnb_raw_data asset
airbnb_ingestion_job = define_asset_job(
    name="airbnb_ingestion_job",
    selection="airbnb_raw_data",
    partitions_def=airbnb_partitions,
)

# Job to materialize silver iceberg tables
airbnb_silver_job = define_asset_job(
    name="airbnb_silver_job",
    selection=["silver_listings", "silver_reviews", "silver_neighbourhoods"],
    partitions_def=airbnb_partitions,
)

defs = Definitions(
    assets=all_assets,
    jobs=[airbnb_ingestion_job, airbnb_silver_job],
    sensors=[
        bronze_sensors.airbnb_data_monitor_sensor,
        silver_sensors.airbnb_bronze_to_silver_sensor,
    ],
    resources={
        "minio": MinIOResource(
            endpoint_url=os.getenv("S3_ENDPOINT", "http://minio:9000"),
            access_key=os.getenv("AWS_ACCESS_KEY_ID", "admin"),
            secret_key=os.getenv("AWS_SECRET_ACCESS_KEY", "password123"),
            region=os.getenv("AWS_REGION", "us-east-1"),
        ),
    },
)
