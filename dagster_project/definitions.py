from dagster import Definitions, asset


@asset
def airbnb_raw_data_check():
    """Initial asset to verify Dagster can see our environment."""
    return "Ready to ingest from Inside Airbnb"


defs = Definitions(
    assets=[airbnb_raw_data_check],
)
