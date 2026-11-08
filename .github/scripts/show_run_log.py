"""CI: print meta.pipeline_runs, so each run's row counts and timings are in the job log."""

import os

import duckdb

with duckdb.connect(os.environ["DUCKDB_PATH"], read_only=True) as con:
    con.sql(
        "select snapshot_date, step, status, row_count, round(seconds, 1) as seconds "
        "from meta.pipeline_runs order by logged_at"
    ).show(max_rows=50)
