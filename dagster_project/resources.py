import boto3
from dagster import ConfigurableResource


class MinIOResource(ConfigurableResource):
    endpoint_url: str
    access_key: str
    secret_key: str
    region: str = "us-east-1"

    def get_client(self):
        return boto3.client(
            "s3",
            endpoint_url=self.endpoint_url,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
            region_name=self.region,
        )

    def ensure_bucket(self, bucket_name: str):
        s3 = self.get_client()
        try:
            s3.head_bucket(Bucket=bucket_name)
        except Exception:
            s3.create_bucket(Bucket=bucket_name)
