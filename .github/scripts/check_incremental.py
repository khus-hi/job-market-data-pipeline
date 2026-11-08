"""CI check: the incremental fct_postings equals a --full-refresh rebuild of the same data.

Snapshots the current (incrementally built) table, rebuilds everything from scratch,
and fails if any row differs. This guards the window/orphan logic in fct_postings.sql.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))  # repo root, for ingestion/
from ingestion.pipeline import main as run_pipeline  # noqa: E402

db = os.environ["DUCKDB_PATH"]
with duckdb.connect(db) as con:
    latest = con.execute("select max(snapshot_date) from raw.adzuna_postings").fetchone()[0]
    con.execute("create or replace table main.fct_incremental as select * from marts.fct_postings")

if run_pipeline(["--date", latest.isoformat(), "--skip-extract", "--full-refresh"]) != 0:
    sys.exit("full-refresh build failed")

with duckdb.connect(db) as con:
    diff = con.execute(
        """
        select count(*) from (
            (select * from main.fct_incremental except select * from marts.fct_postings)
            union all
            (select * from marts.fct_postings except select * from main.fct_incremental)
        )
        """
    ).fetchone()[0]
    rows = con.execute("select count(*) from marts.fct_postings").fetchone()[0]
    con.execute("drop table main.fct_incremental")

print(f"fct_postings: {rows} rows, {diff} differ between incremental and full refresh")
sys.exit(1 if diff else 0)
