import os
import requests
import boto3
import unicodedata
from datetime import datetime
from bs4 import BeautifulSoup
from dagster import (
    sensor,
    RunRequest,
    SensorEvaluationContext,
    DefaultSensorStatus,
    SensorResult,
)
from partitions import airbnb_partitions

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}


def normalize_string(s: str) -> str:
    """Normalize string by removing accents, spaces to underscores, and lowercasing."""
    if not s:
        return ""
    # Normalize unicode to decompose combined characters (e.g. 'ü' -> 'u' + '¨')
    nksfd = unicodedata.normalize("NFKD", s)
    # Filter out non-spacing mark (the accents) and join back
    ascii_only = "".join([c for c in nksfd if not unicodedata.combining(c)])
    return ascii_only.replace(" ", "_").lower()


@sensor(
    job_name="airbnb_ingestion_job",
    default_status=DefaultSensorStatus.RUNNING,
    minimum_interval_seconds=300,  # Run every 5 minutes
)
def airbnb_data_monitor_sensor(context: SensorEvaluationContext):
    source_url = os.getenv("SOURCE_URL", "https://insideairbnb.com/get-the-data/")
    bucket = os.getenv("MINIO_BUCKET_NAME", "airbnb-data")
    delay = int(os.getenv("DOWNLOAD_DELAY_SECONDS", "5"))
    max_retries = int(os.getenv("MAX_RETRIES_PER_FILE", "3"))

    # S3 Client for checking existing files
    s3 = boto3.client(
        "s3",
        endpoint_url=os.getenv("S3_ENDPOINT", "http://minio:9000"),
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "admin"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "password123"),
        region_name=os.getenv("AWS_REGION", "us-east-1"),
    )

    try:
        response = requests.get(source_url, timeout=30, headers=HEADERS)
        response.raise_for_status()
    except Exception as e:
        context.log.error(f"Failed to fetch {source_url}: {e}")
        return

    # Explicitly decode as utf-8 to avoid encoding issues with special characters (e.g. Zürich)
    html_content = response.content.decode("utf-8", errors="replace")
    soup = BeautifulSoup(html_content, "html.parser")
    run_requests = []
    new_partitions = []

    # Get existing partitions to avoid redundant adds
    existing_partitions = context.instance.get_dynamic_partitions("airbnb_city_date")

    h3_elements = soup.find_all("h3")
    for h3 in h3_elements:
        location_text = h3.get_text(strip=True)
        parts = [p.strip() for p in location_text.split(",")]
        city = parts[0]
        country = parts[-1]

        h4 = h3.find_next_sibling("h4")
        if not h4:
            continue

        date_str = h4.get_text(strip=True).split("(")[0].strip()
        try:
            current_date = datetime.strptime(date_str, "%d %B, %Y").strftime("%Y-%m-%d")
        except ValueError:
            continue

        partition_key = f"{city}|{country}|{current_date}"

        # Register partition if it's new
        if partition_key not in existing_partitions:
            new_partitions.append(partition_key)

        # Check if missing any files in S3
        prefix = f"{normalize_string(country)}/{normalize_string(city)}/{current_date}/"
        try:
            res = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
            count = len(res.get("Contents", []))
        except Exception:
            count = 0

        # Only trigger if we don't have all 4 files
        if count < 4:
            run_config = {
                "ops": {
                    "airbnb_raw_data": {
                        "config": {
                            "delay": delay,
                            "bucket": bucket,
                            "max_retries": max_retries,
                        }
                    }
                }
            }
            # Adding a timestamp-hour to allow the sensor to retry once an hour if still missing files.
            # As requested: only add the date part if not all files (count < 4) were downloaded.
            run_key = f"{partition_key}_{count}_{datetime.now().strftime('%Y%m%d_%H')}"
            run_requests.append(
                RunRequest(
                    run_key=run_key,
                    partition_key=partition_key,
                    run_config=run_config,
                )
            )

    return SensorResult(
        run_requests=run_requests,
        dynamic_partitions_requests=(
            [airbnb_partitions.build_add_request(new_partitions)]
            if new_partitions
            else []
        ),
    )
