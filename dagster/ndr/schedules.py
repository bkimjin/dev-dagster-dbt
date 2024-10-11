from dagster import ScheduleDefinition

from .jobs import jira

schedules = [
    ScheduleDefinition(
        job=jira.run_all_jira_assets,
        cron_schedule="0 5 * * 1-5",
    )
]
