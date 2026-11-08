"""The metrics command reads real numbers out of the run logs."""

from __future__ import annotations

import duckdb

from ingestion.metrics import collect, to_markdown


def test_metrics_from_a_real_warehouse(sample_warehouse):
    db = sample_warehouse / "w.duckdb"
    m = collect(db)
    with duckdb.connect(str(db), read_only=True) as con:
        fct = con.execute("select count(*) from marts.fct_postings").fetchone()[0]
        loads = con.execute(
            "select row_count from meta.pipeline_runs where step = 'load' order by snapshot_date"
        ).fetchall()

    assert m["days"] == 2 and m["unique_postings"] == fct
    assert m["rows_per_day"][0] == min(r[0] for r in loads)
    assert m["daily_runs"] == 2 and m["runtime"][0] > 0
    assert m["dbt_tests"][0] == m["dbt_tests"][1] > 0  # every data test passed
    assert m["spark"] is None  # Spark job not run here

    table = to_markdown(m)
    assert "| Days collected | 2 (2026-11-02 to 2026-11-03) |" in table
    assert "Spark" not in table and "SLA" not in table  # rows without data are left out
