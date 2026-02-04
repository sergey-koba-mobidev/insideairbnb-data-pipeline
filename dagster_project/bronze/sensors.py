import os
from dagster import (
    sensor,
    SensorEvaluationContext,
    RunRequest,
    AddDynamicPartitionsRequest,
    DefaultSensorStatus,
)
from resources import MinIOResource
from partitions import airbnb_partitions
from shared.utils import normalize_string
from bronze.parsers import InsideAirbnbParser


@sensor(
    job_name="airbnb_ingestion_job",
    minimum_interval_seconds=300,
    default_status=DefaultSensorStatus.RUNNING,
)
def airbnb_data_monitor_sensor(context: SensorEvaluationContext, minio: MinIOResource):
    source_url = os.getenv("SOURCE_URL", "https://insideairbnb.com/get-the-data/")
    parser = InsideAirbnbParser(source_url)
    all_datasets = parser.get_all_partitions()

    run_requests = []
    partition_requests = []

    s3 = minio.get_client()
    bucket = os.getenv("MINIO_BUCKET_NAME", "airbnb-data")

    for ds in all_datasets:
        city, country, date = ds["city"], ds["country"], ds["date"]
        partition_key = f"{city}|{country}|{date}"

        partition_requests.append(partition_key)

        prefix = f"{normalize_string(country)}/{normalize_string(city)}/{date}/"
        res = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
        file_count = len(res.get("Contents", []))

        if file_count < 4:
            from datetime import datetime

            hour_str = datetime.now().strftime("%Y-%m-%d-%H")
            run_key = f"{partition_key}-{hour_str}"

            run_requests.append(
                RunRequest(
                    run_key=run_key,
                    partition_key=partition_key,
                    run_config={
                        "ops": {
                            "airbnb_raw_data": {
                                "config": {
                                    "delay": int(
                                        os.getenv("DOWNLOAD_DELAY_SECONDS", "5")
                                    ),
                                    "bucket": bucket,
                                    "max_retries": int(
                                        os.getenv("MAX_RETRIES_PER_FILE", "3")
                                    ),
                                }
                            }
                        }
                    },
                )
            )

    return (
        [
            AddDynamicPartitionsRequest(
                partitions_def_name=airbnb_partitions.name,
                partition_keys=list(set(partition_requests)),
            )
        ]
        if partition_requests
        else []
    ) + run_requests
