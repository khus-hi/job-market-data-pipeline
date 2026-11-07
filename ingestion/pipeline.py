"""Run one day of the pipeline end to end from the command line:

    extract (API -> raw JSON) -> load (JSON -> DuckDB raw) -> dbt build -> freshness -> report

In production the same steps run as Airflow tasks (dags/job_market_daily.py).

Usage:
    python -m ingestion.pipeline                       # today, real Adzuna API
    python -m ingestion.pipeline --source sample       # fake data, no keys needed
    python -m ingestion.pipeline --date 2026-11-05     # past day: reuses its landed raw files
    python -m ingestion.pipeline --skip-extract        # reload today's raw files, no API calls
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
import uuid
from datetime import UTC, date, datetime

from ingestion.config import get_settings, get_store
from ingestion.quality import log_step
from ingestion.report import top_skills
from ingestion.steps import dbt_build_step, extract_step, freshness_step, load_step

log = logging.getLogger("pipeline")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run the job market pipeline for one day.")
    parser.add_argument("--date", type=date.fromisoformat, default=datetime.now(UTC).date())
    parser.add_argument("--source", choices=["adzuna", "sample"], default="adzuna")
    parser.add_argument("--skip-extract", action="store_true", help="reuse landed raw files")
    parser.add_argument("--full-refresh", action="store_true", help="rebuild incremental models")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    s = get_settings()
    store = get_store(s)
    run_id = f"cli-{datetime.now(UTC):%Y%m%dT%H%M%S}-{uuid.uuid4().hex[:6]}"
    log.info("run_id=%s date=%s source=%s raw=%s", run_id, args.date, args.source,
             store.describe(args.date))  # fmt: skip
    s.duckdb_path.parent.mkdir(parents=True, exist_ok=True)
    started = time.monotonic()

    # Sample mode is fake data, so "today only" doesn't apply: always generate the day.
    today = args.date if args.source == "sample" else None
    status = "failed"
    try:
        if not args.skip_extract:
            extract_step(s, store, args.date, run_id, args.source, today=today)
        load_step(s, store, args.date, run_id)
        # DuckDB allows one writer at a time: every Python connection above is closed
        # before dbt opens the file.
        dbt_build_step(s, args.date, run_id, args.full_refresh)
        freshness_step(s, args.date, run_id)
        status = "success"
    except Exception as exc:
        log.error("%s", exc)
        return 1
    finally:
        log_step(s.duckdb_path, run_id, args.date, "total", status, None,
                 time.monotonic() - started)  # fmt: skip

    print("\nTop skills this week (share of postings that mention the skill):")
    for week, skill, n, share in top_skills(s.duckdb_path):
        print(f"  {week}  {skill:<18} {n:>5}  {share:.1%}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
