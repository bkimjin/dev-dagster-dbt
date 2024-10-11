# Instructions from https://docs.dagster.io/integrations/fivetran
from dagster_fivetran import FivetranResource, build_fivetran_assets, load_assets_from_fivetran_instance
from dagster import EnvVar, with_resources, AssetKey

# Fivetran configuration
fivetran_instance = FivetranResource(
    api_key=EnvVar("FIVETRAN_API_KEY"),
    api_secret=EnvVar("FIVETRAN_API_SECRET"),
)

# Automatically Load Fivetran assets
# This method loads all connectors from Fivetran into the Dagster graph.

fivetran_assets = load_assets_from_fivetran_instance(
    fivetran_instance,
    connector_filter=lambda meta: "snowflake" in meta.service and "fivetran_metadata" not in meta.name,
)
