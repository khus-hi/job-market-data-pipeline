from datetime import date

import duckdb
import pytest

from ingestion.config import Settings
from ingestion.extract import extract_day
from ingestion.quality import log_step
from ingestion.steps import (
    FETCH,
    MISSING,
    REUSE,
    MissingHistoryError,
    extract_step,
    plan_extract,
    sla_step,
)

TODAY = date(2026, 11, 7)
PAST = date(2026, 11, 5)


@pytest.mark.parametrize(
    "dt, has_partition, expected",
    [
        (TODAY, False, FETCH),
        (TODAY, True, FETCH),  # re-running today refreshes it
        (PAST, True, REUSE),  # backfill: reprocess what was landed that day
        (PAST, False, MISSING),  # can't fetch the past from Adzuna
    ],
)
def test_plan_extract(dt, has_partition, expected):
    assert plan_extract(dt, TODAY, has_partition) == expected


def settings(tmp_path) -> Settings:
    return Settings(None, None, tmp_path, tmp_path / "w.duckdb", max_pages=5, max_days_old=2)


def test_past_day_with_landed_data_is_reused_without_calling_the_api(tmp_path, store, fake_source):
    extract_day(fake_source, PAST, store, max_pages=5, roles=["data engineer"])
    result = extract_step(settings(tmp_path), store, PAST, "run1", "adzuna", today=TODAY)
    assert result == {"action": REUSE}  # no AdzunaClient was built, so no keys were needed


def test_past_day_without_data_fails_clearly(tmp_path, store):
    with pytest.raises(MissingHistoryError, match="can't return postings for past days"):
        extract_step(settings(tmp_path), store, PAST, "run1", "adzuna", today=TODAY)


def test_sla_step_records_met_and_missed(tmp_path):
    s = settings(tmp_path)
    log_step(s.duckdb_path, "fast", TODAY, "load", "success", 1, 2.0)
    assert sla_step(s, TODAY, "fast", sla_seconds=600)["sla_met"] is True
    log_step(s.duckdb_path, "slow", TODAY, "load", "success", 1, 2.0)
    assert sla_step(s, TODAY, "slow", sla_seconds=0)["sla_met"] is False
    with duckdb.connect(str(s.duckdb_path), read_only=True) as con:
        rows = con.execute(
            "select run_id, status from meta.pipeline_runs where step = 'sla' order by 1"
        ).fetchall()
    assert rows == [("fast", "met"), ("slow", "missed")]


def test_extract_works_when_warehouse_folder_does_not_exist_yet(tmp_path, store, fake_source):
    # With S3 storage nothing creates the local data folder before the run log is written
    # (this broke the first Airflow run).
    s = Settings(None, None, tmp_path, tmp_path / "new" / "dir" / "w.duckdb", 5, 2)
    extract_day(fake_source, PAST, store, max_pages=5, roles=["data engineer"])
    assert extract_step(s, store, PAST, "run1", "adzuna", today=TODAY) == {"action": REUSE}
    assert s.duckdb_path.exists()
