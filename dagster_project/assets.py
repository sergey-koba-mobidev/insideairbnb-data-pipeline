import time
import requests
import os
import unicodedata
from dagster import asset, AssetExecutionContext, Config, MetadataValue
from resources import MinIOResource
from partitions import airbnb_partitions

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}


class AirbnbDownloadConfig(Config):
    delay: int
    bucket: str
    max_retries: int


def normalize_string(s: str) -> str:
    """Normalize string by removing accents, spaces to underscores, and lowercasing."""
    if not s:
        return ""
    nksfd = unicodedata.normalize("NFKD", s)
    ascii_only = "".join([c for c in nksfd if not unicodedata.combining(c)])
    return ascii_only.replace(" ", "_").lower()


@asset(partitions_def=airbnb_partitions, group_name="bronze", compute_kind="python")
def airbnb_raw_data(
    context: AssetExecutionContext, config: AirbnbDownloadConfig, minio: MinIOResource
):
    # The partition key is "city|country|date" (using pipe as separator)
    partition_key = context.partition_key
    city, country, date = partition_key.split("|")

    # Repair city/country if they look like mangled UTF-8 from previous runs
    try:
        # If city was mis-decoded as latin-1, this will fix it (e.g. zÃ¼rich -> zürich)
        city_fixed = city.encode("latin-1").decode("utf-8")
        country_fixed = country.encode("latin-1").decode("utf-8")
        if city_fixed != city or country_fixed != country:
            context.log.info(
                f"Repaired mangled partition metadata: {city} -> {city_fixed}"
            )
            city, country = city_fixed, country_fixed
    except (UnicodeEncodeError, UnicodeDecodeError):
        # Already correct or contains non-latin-1 characters
        pass

    # We need to re-scrape or get URLs from somewhere.
    # To keep it simple, the sensor will pass URLs through metadata or config?
    # Actually, assets shouldn't strictly depend on external config for URLs if possible,
    # but here the "truth" is the website.

    # For now, we'll re-extract URLs for this specific partition in the asset
    # to ensure it's self-contained if re-run manually.
    source_url = os.getenv("SOURCE_URL", "https://insideairbnb.com/get-the-data/")
    response = requests.get(source_url, timeout=30, headers=HEADERS)
    # Explicitly decode as utf-8 to avoid encoding issues with special characters (e.g. Zürich)
    html_content = response.content.decode("utf-8", errors="replace")

    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html_content, "html.parser")

    # Logic to find exact table for city/date
    target_urls = {}
    h3s = soup.find_all("h3")
    found = False
    from datetime import datetime

    for h3 in h3s:
        # Check if city and country match the H3 header
        h3_text = h3.get_text()
        if city in h3_text and country in h3_text:
            h4 = h3.find_next_sibling("h4")
            while h4 and h4.name != "h3":
                if h4.name == "h4":
                    h4_text = h4.get_text(strip=True).split("(")[0].strip()
                    try:
                        # Convert "14 September, 2025" to "2025-09-14"
                        h4_date = datetime.strptime(h4_text, "%d %B, %Y").strftime(
                            "%Y-%m-%d"
                        )
                        if h4_date == date:
                            table = h4.find_next_sibling("table")
                            if table:
                                targets = [
                                    "listings.csv",
                                    "reviews.csv",
                                    "neighbourhoods.csv",
                                    "neighbourhoods.geojson",
                                ]
                                rows = table.find_all("tr")
                                for row in rows:
                                    link = row.find("a")
                                    if link and "href" in link.attrs:
                                        href = link["href"]
                                        for t in targets:
                                            if href.endswith(t) and (
                                                t.endswith(".gz")
                                                or not href.endswith(".gz")
                                            ):
                                                if t.endswith(".csv") and href.endswith(
                                                    ".csv.gz"
                                                ):
                                                    continue
                                                target_urls[t] = href
                                found = True
                                break
                    except ValueError:
                        pass
                h4 = h4.find_next_sibling()
        if found:
            break

    if not target_urls:
        raise ValueError(f"Could not find URLs for partition {partition_key}")

    # Ensure bucket exists
    minio.ensure_bucket(config.bucket)
    s3 = minio.get_client()

    prefix = f"{normalize_string(country)}/{normalize_string(city)}/{date}/"

    # Determine already existing files to skip
    res = s3.list_objects_v2(Bucket=config.bucket, Prefix=prefix)
    existing_keys = [obj["Key"] for obj in res.get("Contents", [])]

    uploaded_keys = []
    for t, url in target_urls.items():
        key = f"{prefix}{t}"
        if key in existing_keys:
            context.log.info(f"Skipping {key} - already exists")
            uploaded_keys.append(key)
            continue

        retries = 0
        success = False
        while retries <= config.max_retries and not success:
            try:
                context.log.info(f"Downloading {url}...")
                with requests.get(url, stream=True, timeout=30, headers=HEADERS) as r:
                    r.raise_for_status()
                    s3.upload_fileobj(r.raw, config.bucket, key)
                context.log.info(f"Successfully uploaded {key}.")
                uploaded_keys.append(key)
                success = True
            except Exception as e:
                retries += 1
                if retries <= config.max_retries:
                    context.log.warning(
                        f"Retry {retries}/{config.max_retries} for {url} due to error: {e}"
                    )
                    time.sleep(2**retries)
                else:
                    context.log.error(
                        f"Failed to download/upload {url} after {config.max_retries} retries: {e}"
                    )
                    raise e

        if success:
            time.sleep(config.delay)

    context.add_output_metadata(
        {
            "files_uploaded": MetadataValue.int(len(uploaded_keys)),
            "s3_path": MetadataValue.path(f"s3://{config.bucket}/{prefix}"),
            "city": city,
            "date": date,
        }
    )
