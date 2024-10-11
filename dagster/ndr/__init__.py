from dagster import Definitions, ExperimentalWarning, load_assets_from_package_module
from dagster_dbt import DbtCliResource
# Assets are placed in a sub directory as there are multiple assets. If there are
# multiple projects and schedules in the future, they may also require subdirectories.
from . import assets, jobs
from .project import ndr_project
from .schedules import schedules

import warnings

# To suppress experimental warnings. Without this, dagster logs becomes unreadable.
warnings.filterwarnings("ignore", category=ExperimentalWarning)

all_assets = load_assets_from_package_module(assets)
all_jobs = [
    jobs.jira.run_all_jira_assets,
]

defs = Definitions(
    assets=all_assets,
    jobs=all_jobs,
    schedules=schedules,
    resources={
        "dbt": DbtCliResource(project_dir=ndr_project),
    },
)
