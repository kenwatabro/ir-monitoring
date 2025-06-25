from __future__ import annotations

from datetime import date

from prefect import flow, task
from prefect.deployments import DeploymentSpec
from prefect.server.schemas.schedules import CronSchedule

from src.runner import run_since


@task(retries=2, retry_delay_seconds=300)
def etl_task(target_date: str) -> None:
    run_since(date.fromisoformat(target_date), 1)


@flow(name="ir_daily_flow")
def ir_daily_flow() -> None:  # noqa: D401
    today = date.today()
    etl_task.submit(today.isoformat())


# Deployment definition (Prefect 2.x style)
DeploymentSpec(
    flow=ir_daily_flow,
    name="daily-0800-jst",
    schedule=CronSchedule(cron="0 23 * * *", timezone="Asia/Tokyo"),  # 08:00 JST == 23:00 UTC prev day
    tags=["ir"],
) 