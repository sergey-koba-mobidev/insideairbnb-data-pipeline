import time
import requests
from typing import Dict, Any


def download_and_upload_with_retry(
    url: str,
    bucket: str,
    key: str,
    s3_client: Any,
    max_retries: int,
    headers: Dict[str, str],
    log: Any,
) -> bool:
    retries = 0
    while retries <= max_retries:
        try:
            log.info(f"Downloading {url}...")
            with requests.get(url, stream=True, timeout=30, headers=headers) as r:
                r.raise_for_status()
                s3_client.upload_fileobj(r.raw, bucket, key)
            log.info(f"Successfully uploaded {key}.")
            return True
        except Exception as e:
            retries += 1
            if retries <= max_retries:
                log.warning(
                    f"Retry {retries}/{max_retries} for {url} due to error: {e}"
                )
                time.sleep(2**retries)
            else:
                log.error(
                    f"Failed to download/upload {url} after {max_retries} retries: {e}"
                )
                raise e
    return False
