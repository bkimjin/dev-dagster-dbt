from pathlib import Path

from dagster_dbt import DbtProject
from dagster_dbt.dbt_project import using_dagster_dev

if using_dagster_dev():
    project_dir = Path(__file__).joinpath("..", "..", "..", "dbt").resolve()
else:
    project_dir = Path(__file__).joinpath("..", "..", "dbt").resolve()

ndr_project = DbtProject(
    project_dir=project_dir,
    packaged_project_dir=Path(__file__).joinpath("..", "..", "..", "dbt-project").resolve(),
)
ndr_project.prepare_if_dev()