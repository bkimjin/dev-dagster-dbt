from dagster import define_asset_job, AssetSelection
from dagster_dbt import build_dbt_asset_selection

from ..assets.dbt import ndr_dbt_build_assets

dbt_view_assets = build_dbt_asset_selection([ndr_dbt_build_assets], "config.materialized:view")

run_all_jira_assets = define_asset_job(
    name="run_all_jira_assets",
    selection=(
        # Use build_dbt_asset_selection to run all models tagged with jira and downstream
        # Dagster assets excluding views.
        (build_dbt_asset_selection([ndr_dbt_build_assets], "test1").downstream() - dbt_view_assets)
    )
)
