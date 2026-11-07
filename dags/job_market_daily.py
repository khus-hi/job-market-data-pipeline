"""Daily job market pipeline: Adzuna -> S3 raw landing zone -> DuckDB -> dbt.

Each pipeline step is its own task, so a failure retries only that step and the UI shows
exactly where a run broke. The work itself lives in `ingestion/steps.py`. This file only
describes *when* and *in what order* the steps run, and what happens when they fail.

    extract -> load -> dbt_build -> source_freshness -> sla_check

Backfill a date range (re-processes raw files already landed for those days):
    astro dev run backfill create --dag-id job_market_daily \
        --from-date 2026-11-07 --to-date 2026-11-11 --reprocess-behavior completed
(--to-date is exclusive of that day's 13:00 run; see docs/airflow.md)
"""

from __future__ import annotations

import logging
from datetime import date, timedelta

import pendulum
from airflow.exceptions import AirflowFailException
from airflow.sdk import dag, get_current_context, task

from ingestion.alerts import task_failed

log = logging.getLogger(__name__)

# SLA: a run should finish within this long after it starts. A normal run takes about a
# minute, so 30 minutes means something is really wrong.
#
# Airflow 3 removed the old per-task `sla=` argument. Its replacement, Deadline Alerts, is
# experimental in Airflow 3.1 and broke `airflow dags test` runs when tried here, so the SLA
# is enforced with plain, version-proof pieces instead:
#   * sla_check task: records "met"/"missed" in meta.pipeline_runs (queryable SLA history)
#   * dagrun_timeout: hard stop that fails a run stuck for over an hour
SLA = timedelta(minutes=30)


def _context() -> tuple:
    """Settings, raw store, snapshot date and run id for the current task."""
    from ingestion.config import get_settings, get_store

    ctx = get_current_context()
    s = get_settings()
    return s, get_store(s), date.fromisoformat(ctx["ds"]), ctx["run_id"]


@dag(
    dag_id="job_market_daily",
    description="Adzuna job postings -> S3 -> DuckDB -> dbt, once a day",
    schedule="0 13 * * *",  # 13:00 UTC = morning in the US, after overnight postings
    start_date=pendulum.datetime(2026, 11, 1, tz="UTC"),
    catchup=False,  # don't auto-run missed days: Adzuna can't return past days anyway
    max_active_runs=1,  # DuckDB allows one writer, so runs (incl. backfills) go one at a time
    dagrun_timeout=timedelta(hours=1),
    default_args={
        "owner": "data-eng",
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
        "retry_exponential_backoff": True,  # 2 min, then ~4 min
        "max_retry_delay": timedelta(minutes=10),
        "on_failure_callback": task_failed,
    },
    tags=["job-market", "adzuna", "dbt"],
    doc_md=__doc__,
)
def job_market_daily():
    @task(retries=3)  # the API is the flakiest part, so it gets one more retry
    def extract() -> dict:
        import os

        from ingestion.steps import MissingHistoryError, extract_step

        s, store, dt, run_id = _context()
        source = os.getenv("PIPELINE_SOURCE", "adzuna")
        today = dt if source == "sample" else None  # fake data can be generated for any day
        try:
            return extract_step(s, store, dt, run_id, source, today=today)
        except MissingHistoryError as exc:
            # Retrying can't create history that was never landed, so fail without retries.
            raise AirflowFailException(str(exc)) from exc

    @task
    def load(extract_result: dict) -> dict:
        from ingestion.steps import load_step

        s, store, dt, run_id = _context()
        return load_step(s, store, dt, run_id)

    @task(retries=0)  # a failing dbt test is a data problem, not a flaky one: don't retry
    def dbt_build(load_result: dict) -> dict:
        from ingestion.steps import dbt_build_step

        s, _, dt, run_id = _context()
        return dbt_build_step(s, dt, run_id)

    @task(retries=0)
    def source_freshness(build_result: dict) -> None:
        from ingestion.steps import freshness_step

        s, _, dt, run_id = _context()
        if freshness_step(s, dt, run_id) != 0:
            raise AirflowFailException("Source freshness check failed: raw data is stale")

    @task(retries=0)
    def sla_check() -> dict:
        """Record in meta.pipeline_runs whether this run met the SLA, so SLA history is
        queryable with SQL, and warn in the logs when it was missed."""
        from ingestion.steps import sla_step

        s, _, dt, run_id = _context()
        return sla_step(s, dt, run_id, SLA.total_seconds())

    freshness = source_freshness(dbt_build(load(extract())))
    freshness >> sla_check()


job_market_daily()
