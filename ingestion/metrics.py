"""Print the project's measured metrics, straight from the warehouse's run logs.

    python -m ingestion.metrics

Every number comes from meta.pipeline_runs, meta.dbt_results or the marts, so it's
measured, never estimated. Output is a Markdown table, ready to paste into the README.
"""

from __future__ import annotations

import json
from pathlib import Path

import duckdb

from ingestion.config import get_settings


def collect(db_path: Path) -> dict:
    with duckdb.connect(str(db_path), read_only=True) as con:

        def one(sql: str, params: list | None = None):
            return con.execute(sql, params or []).fetchone()

        days, first_day, last_day = one(
            "select count(distinct snapshot_date), min(snapshot_date), max(snapshot_date) "
            "from raw.adzuna_postings"
        )
        # Raw rows per day = the size of each day's load (the latest load of that day).
        rows_min, rows_avg, rows_max = one(
            """
            with latest as (
                select snapshot_date, row_count from meta.pipeline_runs
                where step = 'load' and status = 'success'
                qualify row_number() over (partition by snapshot_date order by logged_at desc) = 1
            )
            select min(row_count), avg(row_count), max(row_count) from latest
            """
        )
        unique_postings = one("select count(*) from marts.fct_postings")[0]
        # A daily run = one run_id that fetched from the API (not a reuse/backfill).
        runs, runtime_median, runtime_max = one(
            """
            with fetch_runs as (
                select run_id from meta.pipeline_runs
                where step = 'extract' and status = 'success'
            ),
            per_run as (
                select run_id, sum(seconds) as seconds
                from meta.pipeline_runs
                where run_id in (select run_id from fetch_runs)
                  and step in ('extract', 'load', 'dbt_build', 'source_freshness')
                group by run_id
            )
            select count(*), median(seconds), max(seconds) from per_run
            """
        )
        failed_steps = one("select count(*) from meta.pipeline_runs where status = 'failed'")[0]
        sla_met, sla_missed = one(
            "select count(*) filter (where status = 'met'), "
            "count(*) filter (where status = 'missed') "
            "from meta.pipeline_runs where step = 'sla'"
        )
        tests, tests_passed = one(
            """
            select count(*), count(*) filter (where status = 'pass')
            from meta.dbt_results
            where resource_type = 'test'
              and run_id = (select run_id from meta.dbt_results order by logged_at desc limit 1)
            """
        )
        spark = one(
            "select details, seconds from meta.pipeline_runs where step = 'spark_dedup' "
            "order by logged_at desc limit 1"
        )

    return {
        "days": days,
        "first_day": first_day,
        "last_day": last_day,
        "rows_per_day": (rows_min, rows_avg, rows_max),
        "unique_postings": unique_postings,
        "daily_runs": runs,
        "runtime": (runtime_median, runtime_max),
        "failed_steps": failed_steps,
        "sla": (sla_met, sla_missed),
        "dbt_tests": (tests, tests_passed),
        "spark": (json.loads(spark[0]), spark[1]) if spark else None,
    }


def to_markdown(m: dict) -> str:
    lo, avg, hi = m["rows_per_day"]
    med, worst = m["runtime"]
    lines = [
        "| Metric | Value |",
        "|---|---|",
        f"| Days collected | {m['days']} ({m['first_day']} to {m['last_day']}) |",
        f"| Raw rows per day | avg {avg:,.0f} (min {lo:,}, max {hi:,}) |" if avg else None,
        f"| Unique postings | {m['unique_postings']:,} |",
        f"| Daily API runs | {m['daily_runs']}, median {med:.1f} s, slowest {worst:.1f} s |"
        if m["daily_runs"]
        else None,
        f"| Failed steps logged | {m['failed_steps']} |",
        f"| SLA (30 min) | met {m['sla'][0]}, missed {m['sla'][1]} |" if sum(m["sla"]) else None,
        f"| dbt data tests (latest build) | {m['dbt_tests'][1]} of {m['dbt_tests'][0]} passed |",
    ]
    if m["spark"]:
        s, secs = m["spark"]
        lines.append(
            f"| Spark dedup | {s['sightings']:,} sightings -> {s['postings']:,} postings -> "
            f"{s['distinct_jobs']:,} distinct jobs ({s['reposts']:,} reposts) over "
            f"{s['days']} days, {secs:.1f} s |"
        )
    return "\n".join(line for line in lines if line)


def main() -> None:
    s = get_settings()
    print(f"Warehouse: {s.duckdb_path}\n")
    print(to_markdown(collect(s.duckdb_path)))


if __name__ == "__main__":
    main()
