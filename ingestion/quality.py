"""Data-quality logging: every run writes row counts and dbt results into the
`meta` schema, so you can answer "what happened on run X?" with a SQL query.

    meta.pipeline_runs  one row per step per run (rows, status, seconds)
    meta.dbt_results    one row per dbt model/test per run (pass/fail, failures)
"""

from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path

import duckdb

DDL = """
create schema if not exists meta;

create table if not exists meta.pipeline_runs (
    run_id       varchar,
    snapshot_date date,
    step         varchar,
    status       varchar,
    row_count    bigint,
    seconds      double,
    details      varchar,
    logged_at    timestamptz
);

create table if not exists meta.dbt_results (
    run_id        varchar,
    snapshot_date date,
    unique_id     varchar,
    resource_type varchar,
    status        varchar,
    failures      bigint,
    seconds       double,
    message       varchar,
    logged_at     timestamptz
);
"""


def log_step(
    db_path: Path,
    run_id: str,
    dt: date,
    step: str,
    status: str,
    row_count: int | None,
    seconds: float,
    details: dict | None = None,
) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(db_path)) as con:
        con.execute(DDL)
        con.execute(
            "insert into meta.pipeline_runs values (?, ?, ?, ?, ?, ?, ?, ?)",
            [
                run_id,
                dt,
                step,
                status,
                row_count,
                round(seconds, 2),
                json.dumps(details or {}),
                datetime.now(UTC),
            ],
        )


def log_dbt_results(db_path: Path, run_id: str, dt: date, run_results: Path) -> dict[str, int]:
    """Copy dbt's target/run_results.json into meta.dbt_results. Returns status counts."""
    data = json.loads(run_results.read_text(encoding="utf-8"))
    now = datetime.now(UTC)
    rows = []
    for r in data.get("results", []):
        uid = r["unique_id"]
        rows.append(
            (
                run_id,
                dt,
                uid,
                uid.split(".")[0],
                r["status"],
                r.get("failures"),
                round(r.get("execution_time") or 0, 3),
                (r.get("message") or "")[:500],
                now,
            )
        )
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(db_path)) as con:
        con.execute(DDL)
        if rows:
            con.executemany("insert into meta.dbt_results values (?, ?, ?, ?, ?, ?, ?, ?, ?)", rows)
    counts: dict[str, int] = {}
    for row in rows:
        counts[row[4]] = counts.get(row[4], 0) + 1
    return counts
