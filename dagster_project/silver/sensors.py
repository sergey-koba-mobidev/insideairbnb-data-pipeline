import os
from dagster import (
    sensor,
    SensorEvaluationContext,
    RunRequest,
    DagsterRunStatus,
    RunsFilter,
    DefaultSensorStatus,
)
from partitions import airbnb_partitions


@sensor(
    job_name="airbnb_silver_job",
    minimum_interval_seconds=100,
    default_status=DefaultSensorStatus.RUNNING,
)
def airbnb_bronze_to_silver_sensor(context: SensorEvaluationContext):
    bucket = os.getenv("MINIO_BUCKET_NAME", "airbnb-data")
    s3_endpoint = os.getenv("S3_ENDPOINT", "http://minio:9000")

    # We'll use boto3 directly here as in original sensor or use a shared client
    import boto3

    s3 = boto3.client(
        "s3",
        endpoint_url=s3_endpoint,
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_REGION", "us-east-1"),
    )

    partition_keys = airbnb_partitions.get_partition_keys(
        dynamic_partitions_store=context.instance
    )

    for partition_key in partition_keys:
        city, country, date = partition_key.split("|")

        # Need normalize_string here too
        from shared.utils import normalize_string

        prefix = f"{normalize_string(country)}/{normalize_string(city)}/{date}/"

        res = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
        file_count = len(res.get("Contents", []))

        if file_count >= 4:
            # Check if silver already exists (simulated by checking iceberg warehouse)
            silver_prefix = f"warehouse/listings/data/partition_country={country}/partition_city={city}/partition_date={date}/"
            res_silver = s3.list_objects_v2(Bucket=bucket, Prefix=silver_prefix)
            if len(res_silver.get("Contents", [])) == 0:
                # Check for active runs
                runs = context.instance.get_runs(
                    filters=RunsFilter(
                        job_name="airbnb_silver_job",
                        statuses=[
                            DagsterRunStatus.STARTED,
                            DagsterRunStatus.STARTING,
                            DagsterRunStatus.QUEUED,
                        ],
                    )
                )
                if any(
                    run.tags.get("dagster/partition") == partition_key for run in runs
                ):
                    continue

                yield RunRequest(
                    run_key=f"silver-{partition_key}",
                    partition_key=partition_key,
                )
