"""The Airflow DAG imports cleanly and is wired the way docs/airflow.md describes.

Needs Airflow installed, so it is skipped in the normal dev environment and runs in its
own CI job (see .github/workflows/ci.yml).
"""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import pytest

pytest.importorskip("airflow")

DAGS_DIR = Path(__file__).resolve().parent.parent / "dags"


@pytest.fixture(scope="module")
def dagbag():
    from airflow.models import DagBag

    # Only our folder is scanned, so Airflow's example DAGs never load here.
    return DagBag(dag_folder=str(DAGS_DIR))


def test_no_import_errors(dagbag):
    assert dagbag.import_errors == {}


def test_job_market_daily_shape(dagbag):
    dag = dagbag.dags.get("job_market_daily")  # .dags, not get_dag(): no metadata DB needed
    assert dag is not None
    assert dag.max_active_runs == 1  # DuckDB has a single writer
    assert dag.catchup is False  # Adzuna can't return past days
    assert dag.dagrun_timeout == timedelta(hours=1)

    tasks = {t.task_id: t for t in dag.tasks}
    assert set(tasks) == {"extract", "load", "dbt_build", "source_freshness", "sla_check"}
    assert tasks["extract"].downstream_task_ids == {"load"}
    assert tasks["load"].downstream_task_ids == {"dbt_build"}
    assert tasks["dbt_build"].downstream_task_ids == {"source_freshness"}
    assert tasks["source_freshness"].downstream_task_ids == {"sla_check"}

    assert tasks["extract"].retries == 3
    assert tasks["load"].retries == 2
    assert tasks["dbt_build"].retries == 0  # a failing data test is not fixed by retrying
