import json
from datetime import date

import duckdb

from ingestion.quality import log_dbt_results, log_step


def test_log_step_and_dbt_results(tmp_path):
    db = tmp_path / "w.duckdb"
    log_step(db, "run1", date(2026, 11, 2), "load", "success", 42, 1.234, {"x": 1})
    results = tmp_path / "run_results.json"
    results.write_text(
        json.dumps(
            {
                "results": [
                    {
                        "unique_id": "model.job_market.fct_postings",
                        "status": "success",
                        "execution_time": 0.5,
                    },
                    {"unique_id": "test.job_market.unique_x", "status": "pass", "failures": 0},
                    {"unique_id": "test.job_market.not_null_y", "status": "fail", "failures": 3},
                ]
            }
        )
    )
    assert log_dbt_results(db, "run1", date(2026, 11, 2), results) == {
        "success": 1,
        "pass": 1,
        "fail": 1,
    }
    with duckdb.connect(str(db), read_only=True) as con:
        assert con.execute("select row_count from meta.pipeline_runs").fetchone()[0] == 42
        assert (
            con.execute("select failures from meta.dbt_results where status = 'fail'").fetchone()[0]
            == 3
        )
