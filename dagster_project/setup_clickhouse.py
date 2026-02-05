import os
import clickhouse_connect


def setup_clickhouse():
    host = os.getenv("CLICKHOUSE_HOST", "clickhouse")
    port = int(os.getenv("CLICKHOUSE_HTTP_PORT", "8123"))
    user = os.getenv("CLICKHOUSE_USER", "default")
    password = os.getenv("CLICKHOUSE_PASSWORD", "clickhouse_pass")

    s3_endpoint = os.getenv("S3_ENDPOINT", "http://minio:9000")
    aws_access_key = os.getenv("AWS_ACCESS_KEY_ID", "admin")
    aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY", "password123")
    bucket = os.getenv("MINIO_BUCKET_NAME", "airbnb-data")

    # For DataLakeCatalog with Nessie REST, the warehouse setting should often be the
    warehouse_name = "nessie"

    print(f"Connecting to ClickHouse at {host}:{port}...")
    client = clickhouse_connect.get_client(
        host=host,
        port=port,
        username=user,
        password=password,
    )

    try:
        print("Setting experimental flag...")
        client.command("SET allow_experimental_database_iceberg = 1")

        print("Creating 'silver' database using DataLakeCatalog...")
        client.command("DROP DATABASE IF EXISTS silver")

        # The key is to use the bare host for storage_endpoint and the logical name for warehouse.
        # This prevents ClickHouse from using the warehouse URI path as a prefix.
        create_sql = f"""
            CREATE DATABASE silver
            ENGINE = DataLakeCatalog('http://nessie:19120/iceberg', '{aws_access_key}', '{aws_secret_key}')
            SETTINGS
                catalog_type = 'rest',
                storage_endpoint = '{s3_endpoint}/{bucket}',
                warehouse = '{warehouse_name}'
        """
        client.command(create_sql)
        print("Database 'silver' created successfully.")

        # Verify
        tables = client.command("SHOW TABLES FROM silver")
        print(f"Discovered tables: {tables}")

    except Exception as e:
        print(f"Error: {e}")
        exit(1)


if __name__ == "__main__":
    setup_clickhouse()
