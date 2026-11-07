"""Load: copy one day of raw JSON files into DuckDB's `raw` schema.

ELT, not ETL: we store each posting's JSON verbatim. Parsing, typing and
cleaning happen later in dbt, where they are versioned, tested and re-runnable.
The one thing done here in Python is skill extraction, because it is regex
logic we unit test with pytest.

Idempotency: inside ONE transaction we delete everything for the date, then
insert it again. Re-running a day gives the same rows, never duplicates. If
anything fails mid-way the transaction rolls back and the old data stays.
"""

from __future__ import annotations

import json
import logging
from datetime import date
from pathlib import Path

import duckdb

from ingestion.skills import extract_skills
from ingestion.storage import RawStore

log = logging.getLogger(__name__)

DDL = """
create schema if not exists raw;

create table if not exists raw.adzuna_postings (
    snapshot_date date      not null,  -- the dt= partition the file came from
    ingested_at   timestamptz not null,
    search_role   varchar   not null,
    page          integer   not null,
    source_file   varchar   not null,
    posting_id    varchar   not null,
    payload       json      not null   -- untouched API record
);

create table if not exists raw.posting_skills (
    snapshot_date date    not null,
    posting_id    varchar not null,
    skill_name    varchar not null
);
"""


def ensure_tables(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(DDL)


def read_partition(store: RawStore, dt: date) -> tuple[list[tuple], list[tuple]]:
    """Parse every file for one date into rows for the two raw tables."""
    posting_rows: list[tuple] = []
    skill_rows: set[tuple] = set()
    for name, body in store.read_partition(dt):
        envelope = json.loads(body)
        for rec in envelope["response"].get("results", []):
            pid = str(rec["id"])
            posting_rows.append(
                (
                    dt,
                    envelope["ingested_at"],
                    envelope["search_role"],
                    envelope["page"],
                    name,
                    pid,
                    json.dumps(rec),
                )
            )
            for skill in extract_skills(rec.get("title"), rec.get("description")):
                skill_rows.add((dt, pid, skill))  # set: one row per posting+skill
    return posting_rows, sorted(skill_rows)


def load_day(db_path: Path, store: RawStore, dt: date) -> dict[str, int]:
    posting_rows, skill_rows = read_partition(store, dt)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(db_path)) as con:
        ensure_tables(con)
        con.begin()
        try:
            con.execute("delete from raw.adzuna_postings where snapshot_date = ?", [dt])
            con.execute("delete from raw.posting_skills where snapshot_date = ?", [dt])
            if posting_rows:
                con.executemany(
                    "insert into raw.adzuna_postings values (?, ?, ?, ?, ?, ?, ?)", posting_rows
                )
            if skill_rows:
                con.executemany("insert into raw.posting_skills values (?, ?, ?)", skill_rows)
            con.commit()
        except Exception:
            con.rollback()
            raise
    log.info("loaded %s: %d posting rows, %d skill rows", dt, len(posting_rows), len(skill_rows))
    return {"posting_rows": len(posting_rows), "skill_rows": len(skill_rows)}
