"""The pipeline's steps as plain functions.

Both entry points call these, so the logic lives in one place:
  * `ingestion/pipeline.py`  runs them in order from the command line
  * `dags/job_market_daily.py` runs each one as its own Airflow task

Every step writes a row to meta.pipeline_runs (see quality.py).
"""

from __future__ import annotations

import logging
import os
import shutil
import subprocess
import time
from datetime import UTC, date, datetime

from ingestion.adzuna_client import AdzunaClient
from ingestion.config import REPO_ROOT, Settings
from ingestion.extract import extract_day
from ingestion.load import load_day
from ingestion.quality import log_dbt_results, log_step
from ingestion.sample_data import SamplePostingSource
from ingestion.storage import RawStore

log = logging.getLogger(__name__)
DBT_DIR = REPO_ROOT / "dbt"

FETCH, REUSE, MISSING = "fetch", "reuse", "missing"


class MissingHistoryError(RuntimeError):
    """A past date has no raw data, and Adzuna cannot return postings for past days."""


def plan_extract(dt: date, today: date, has_partition: bool) -> str:
    """Decide what extract should do for snapshot date `dt`.

    Adzuna only returns postings that are live *now*. Fetching "for" a past date
    would label today's postings with the wrong date and corrupt the history.
      * today            -> fetch from the API (replacing any earlier run today)
      * past, data exists -> reuse the raw files already landed (backfill / re-run)
      * past, no data     -> missing: nothing can be done honestly, so fail loudly
    """
    if dt == today:
        return FETCH
    return REUSE if has_partition else MISSING


def extract_step(
    s: Settings,
    store: RawStore,
    dt: date,
    run_id: str,
    source_name: str = "adzuna",
    today: date | None = None,
) -> dict:
    today = today or datetime.now(UTC).date()
    action = plan_extract(dt, today, dt in store.list_partitions())
    if action == MISSING:
        raise MissingHistoryError(
            f"No raw data for {dt} at {store.describe(dt)}, and Adzuna can't return postings "
            "for past days. Only dates that were landed on the day can be (re)processed."
        )
    if action == REUSE:
        log.info("reusing raw files already landed for %s", dt)
        log_step(s.duckdb_path, run_id, dt, "extract", "skipped", None, 0, {"action": action})
        return {"action": action}

    t = time.monotonic()
    if source_name == "sample":
        source = SamplePostingSource(dt, s.results_per_page)
    else:
        source = AdzunaClient(s.adzuna_app_id, s.adzuna_app_key, s.results_per_page, s.max_days_old)
    stats = extract_day(source, dt, store, s.max_pages)
    details = {**stats, "action": action, "source": source_name, "raw": store.describe(dt)}
    log_step(
        s.duckdb_path, run_id, dt, "extract", "success", stats["postings"],
        time.monotonic() - t, details,
    )  # fmt: skip
    return details


def load_step(s: Settings, store: RawStore, dt: date, run_id: str) -> dict:
    t = time.monotonic()
    stats = load_day(s.duckdb_path, store, dt)
    log_step(
        s.duckdb_path, run_id, dt, "load", "success", stats["posting_rows"],
        time.monotonic() - t, stats,
    )  # fmt: skip
    return stats


def _dbt_executable() -> str:
    # In the Airflow image dbt lives in its own virtualenv (DBT_EXECUTABLE points at it)
    # so its dependencies can't clash with Airflow's.
    exe = os.getenv("DBT_EXECUTABLE") or shutil.which("dbt")
    if not exe:
        raise RuntimeError("dbt not found. Activate the virtualenv or set DBT_EXECUTABLE.")
    return exe


def run_dbt(*args: str) -> int:
    cmd = [_dbt_executable(), *args, "--project-dir", str(DBT_DIR), "--profiles-dir", str(DBT_DIR)]
    log.info("running: dbt %s", " ".join(args))
    return subprocess.run(cmd, cwd=REPO_ROOT).returncode


def dbt_build_step(s: Settings, dt: date, run_id: str, full_refresh: bool = False) -> dict:
    """dbt build = seed + run + test, in dependency order. Fails if any test fails."""
    t = time.monotonic()
    rc = run_dbt("build", *(["--full-refresh"] if full_refresh else []))
    counts = log_dbt_results(s.duckdb_path, run_id, dt, DBT_DIR / "target" / "run_results.json")
    status = "success" if rc == 0 else "failed"
    log_step(s.duckdb_path, run_id, dt, "dbt_build", status, None, time.monotonic() - t, counts)
    log.info("dbt build results: %s", counts)
    if rc != 0:
        raise RuntimeError(f"dbt build failed {counts}; see meta.dbt_results for details")
    return counts


def freshness_step(s: Settings, dt: date, run_id: str) -> int:
    t = time.monotonic()
    rc = run_dbt("source", "freshness")
    status = "success" if rc == 0 else "failed"
    log_step(s.duckdb_path, run_id, dt, "source_freshness", status, None, time.monotonic() - t)
    return rc


def sla_step(s: Settings, dt: date, run_id: str, sla_seconds: float) -> dict:
    """Record whether this run met its SLA. Elapsed time = from the start of the run's first
    logged step until now, measured from meta.pipeline_runs, so it means the same thing for
    scheduled runs, manual runs, backfills and `airflow dags test`."""
    import duckdb

    # Computed in SQL as plain seconds, so no timezone objects cross into Python.
    with duckdb.connect(str(s.duckdb_path), read_only=True) as con:
        elapsed = con.execute(
            "select epoch(now()) - epoch(min(logged_at - to_seconds(seconds))) "
            "from meta.pipeline_runs where run_id = ?",
            [run_id],
        ).fetchone()[0]
    elapsed = float(elapsed or 0.0)
    met = elapsed <= sla_seconds
    log_step(s.duckdb_path, run_id, dt, "sla", "met" if met else "missed", None, elapsed,
             {"sla_seconds": sla_seconds})  # fmt: skip
    if not met:
        log.warning("SLA missed: run took %.0fs (limit %.0fs)", elapsed, sla_seconds)
    return {"elapsed_seconds": round(elapsed, 1), "sla_met": met}
