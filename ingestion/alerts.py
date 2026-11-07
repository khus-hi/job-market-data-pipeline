"""Alert callbacks used by Airflow.

In production these would page someone (Slack, PagerDuty, email). Here they write a clear
warning to the Airflow logs, which is enough to show the mechanism without paid services.
"""

from __future__ import annotations

import logging

log = logging.getLogger("job_market.alerts")


def task_failed(context) -> None:
    """on_failure_callback: runs after a task has used up all of its retries."""
    ti = context.get("task_instance")
    log.error(
        "TASK FAILED after retries: dag=%s task=%s run=%s",
        getattr(ti, "dag_id", "?"), getattr(ti, "task_id", "?"), context.get("run_id"),
    )  # fmt: skip
