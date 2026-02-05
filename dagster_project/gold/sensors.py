from dagster import (
    RunRequest,
    SensorEvaluationContext,
    asset_sensor,
    AssetKey,
    DefaultSensorStatus,
)


@asset_sensor(
    asset_key=AssetKey("silver_listings"),
    job_name="airbnb_gold_job",
    default_status=DefaultSensorStatus.RUNNING,
)
def airbnb_silver_to_gold_sensor(context: SensorEvaluationContext, asset_event):
    # This sensor triggers the Gold job for the same partition when silver_listings is materialized
    partition_key = asset_event.dagster_event.partition
    return RunRequest(
        run_key=f"gold_{partition_key}",
        partition_key=partition_key,
    )
