import time
import os
from dagster import (
    asset,
    AssetExecutionContext,
    Config,
    MetadataValue,
)
from resources import MinIOResource
from partitions import airbnb_partitions
from shared.utils import normalize_string, repair_mangled_string
from shared.constants import HEADERS
from shared.downloader import download_and_upload_with_retry
from bronze.parsers import InsideAirbnbParser


class AirbnbDownloadConfig(Config):
    delay: int
    bucket: str
    max_retries: int


@asset(partitions_def=airbnb_partitions, group_name="bronze", compute_kind="python")
def airbnb_raw_data(
    context: AssetExecutionContext, config: AirbnbDownloadConfig, minio: MinIOResource
):
    partition_key = context.partition_key
    city, country, date = partition_key.split("|")

    city = repair_mangled_string(city)
    country = repair_mangled_string(country)

    source_url = os.getenv("SOURCE_URL", "https://insideairbnb.com/get-the-data/")
    parser = InsideAirbnbParser(source_url)
    target_urls = parser.get_urls_for_partition(city, country, date)

    if not target_urls:
        raise ValueError(f"Could not find URLs for partition {partition_key}")

    minio.ensure_bucket(config.bucket)
    s3 = minio.get_client()

    prefix = f"{normalize_string(country)}/{normalize_string(city)}/{date}/"

    res = s3.list_objects_v2(Bucket=config.bucket, Prefix=prefix)
    existing_keys = [obj["Key"] for obj in res.get("Contents", [])]

    uploaded_keys = []
    for t, url in target_urls.items():
        key = f"{prefix}{t}"
        if key in existing_keys:
            context.log.info(f"Skipping {key} - already exists")
            uploaded_keys.append(key)
            continue

        if download_and_upload_with_retry(
            url=url,
            bucket=config.bucket,
            key=key,
            s3_client=s3,
            max_retries=config.max_retries,
            headers=HEADERS,
            log=context.log,
        ):
            uploaded_keys.append(key)
            time.sleep(config.delay)

    context.add_output_metadata(
        {
            "files_uploaded": MetadataValue.int(len(uploaded_keys)),
            "s3_path": MetadataValue.path(f"s3://{config.bucket}/{prefix}"),
            "city": city,
            "date": date,
        }
    )
