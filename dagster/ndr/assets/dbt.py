from dagster import AssetExecutionContext
from dagster_dbt import DagsterDbtTranslator, DbtCliResource, dbt_assets

from ..project import ndr_project

# This class overrides the default DagsterDbtTranslator package
class CustomizedDagsterDbtTranslator(DagsterDbtTranslator):
    def get_asset_key(self, dbt_resource_props):
        return super().get_asset_key(dbt_resource_props)

@dbt_assets(
    manifest=ndr_project.manifest_path,
    dagster_dbt_translator=CustomizedDagsterDbtTranslator()
)
def ndr_dbt_build_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream().fetch_column_metadata()
