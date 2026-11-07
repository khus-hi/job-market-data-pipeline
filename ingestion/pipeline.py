"""Run one day of the pipeline end to end:

    extract (API -> raw JSON) -> load (JSON -> DuckDB raw) -> dbt build -> log -> report

Usage:
    python -m ingestion.pipeline                       # today, real Adzuna API
    python -m ingestion.pipeline --source sample       # fake data, no keys needed
    python -m ingestion.pipeline --date 2026-11-05 --skip-extract   # reload existing files
"""

from __future__ import annotations

import argparse
import logging
import shutil
import subprocess
import sys
import time
import uuid
from datetime import UTC, date, datetime

from ingestion.adzuna_client import AdzunaClient
from ingestion.config import REPO_ROOT, get_settings
from ingestion.extract import extract_day
from ingestion.load import load_day
from ingestion.quality import log_dbt_results, log_step
from ingestion.report import top_skills
from ingestion.sample_data import SamplePostingSource

log = logging.getLogger("pipeline")
DBT_DIR = REPO_ROOT / "dbt"


def run_dbt(*args: str) -> int:
    dbt = shutil.which("dbt")  # finds dbt.exe on Windows too
    if dbt is None:
        raise RuntimeError("dbt not found. Activate the virtualenv first.")
    cmd = [dbt, *args, "--project-dir", str(DBT_DIR), "--profiles-dir", str(DBT_DIR)]
    log.info("running: %s", " ".join(cmd[:2]))
    return subprocess.run(cmd, cwd=REPO_ROOT).returncode


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run the job market pipeline for one day.")
    parser.add_argument("--date", type=date.fromisoformat, default=datetime.now(UTC).date())
    parser.add_argument("--source", choices=["adzuna", "sample"], default="adzuna")
    parser.add_argument("--skip-extract", action="store_true", help="reuse raw files on disk")
    parser.add_argument("--full-refresh", action="store_true", help="rebuild incremental models")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    s = get_settings()
    run_id = f"{datetime.now(UTC):%Y%m%dT%H%M%S}-{uuid.uuid4().hex[:6]}"
    log.info("run_id=%s date=%s source=%s", run_id, args.date, args.source)
    s.duckdb_path.parent.mkdir(parents=True, exist_ok=True)
    started = time.monotonic()

    # 1. Extract
    if not args.skip_extract:
        t = time.monotonic()
        if args.source == "sample":
            source = SamplePostingSource(args.date, s.results_per_page)
        else:
            source = AdzunaClient(
                s.adzuna_app_id, s.adzuna_app_key, s.results_per_page, s.max_days_old
            )
        stats = extract_day(source, args.date, s.raw_dir, s.max_pages)
        log_step(
            s.duckdb_path,
            run_id,
            args.date,
            "extract",
            "success",
            stats["postings"],
            time.monotonic() - t,
            {**stats, "source": args.source},
        )

    # 2. Load
    t = time.monotonic()
    stats = load_day(s.duckdb_path, s.raw_dir, args.date)
    log_step(
        s.duckdb_path,
        run_id,
        args.date,
        "load",
        "success",
        stats["posting_rows"],
        time.monotonic() - t,
        stats,
    )

    # 3. Transform + test. DuckDB allows one writer at a time, so every Python
    #    connection above is already closed before dbt opens the file.
    t = time.monotonic()
    dbt_args = ["build"] + (["--full-refresh"] if args.full_refresh else [])
    rc = run_dbt(*dbt_args)
    counts = log_dbt_results(
        s.duckdb_path, run_id, args.date, DBT_DIR / "target" / "run_results.json"
    )
    log_step(
        s.duckdb_path,
        run_id,
        args.date,
        "dbt_build",
        "success" if rc == 0 else "failed",
        None,
        time.monotonic() - t,
        counts,
    )
    log.info("dbt build results: %s", counts)

    fresh_rc = run_dbt("source", "freshness")
    log_step(
        s.duckdb_path,
        run_id,
        args.date,
        "source_freshness",
        "success" if fresh_rc == 0 else "failed",
        None,
        0,
    )

    log_step(
        s.duckdb_path,
        run_id,
        args.date,
        "total",
        "success" if rc == 0 else "failed",
        None,
        time.monotonic() - started,
    )
    if rc != 0:
        log.error("dbt build failed; see dbt/target/run_results.json or meta.dbt_results")
        return rc

    # 4. Report
    print("\nTop skills this week (share of postings that mention the skill):")
    for week, skill, n, share in top_skills(s.duckdb_path):
        print(f"  {week}  {skill:<18} {n:>5}  {share:.1%}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
