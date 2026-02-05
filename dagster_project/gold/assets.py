import os
from pathlib import Path
from dagster import AssetExecutionContext
from dagster_dbt import DbtCliResource, dbt_assets, DbtProject, DagsterDbtTranslator
from partitions import airbnb_partitions

# Path to dbt project
DBT_PROJECT_DIR = Path(__file__).joinpath("..", "..", "dbt_project").resolve()

# Create DbtProject instance
# This allows us to use compile-time manifest if available, or generate it
dbt_project = DbtProject(project_dir=os.fspath(DBT_PROJECT_DIR))
dbt_project.prepare_if_dev()


class AirbnbDbtTranslator(DagsterDbtTranslator):
    def get_partitions_def(self, dbt_resource_props):
        return airbnb_partitions


@dbt_assets(
    manifest=dbt_project.manifest_path,
    dagster_dbt_translator=AirbnbDbtTranslator(),
)
def airbnb_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()
