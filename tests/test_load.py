from datetime import date

import duckdb
import pytest

from ingestion.extract import extract_day
from ingestion.load import load_day

DT = date(2026, 11, 2)
ROLES = ["data engineer", "data analyst"]


def counts(db):
    with duckdb.connect(str(db), read_only=True) as con:
        postings = con.execute("select count(*) from raw.adzuna_postings").fetchone()[0]
        skills = con.execute("select count(*) from raw.posting_skills").fetchone()[0]
    return postings, skills


def test_load_inserts_rows_and_skills(tmp_path, fake_source):
    raw, db = tmp_path / "raw", tmp_path / "w.duckdb"
    extract_day(fake_source, DT, raw, max_pages=5, roles=ROLES)
    stats = load_day(db, raw, DT)
    # 5 rows: posting "1" appears under two searches; staging dedupes it, raw keeps both
    assert stats["posting_rows"] == 5
    with duckdb.connect(str(db), read_only=True) as con:
        skills = con.execute(
            "select skill_name from raw.posting_skills where posting_id = '2' order by 1"
        ).fetchall()
    assert [s[0] for s in skills] == ["Airflow", "Spark", "dbt"]


def test_reloading_same_day_is_idempotent(tmp_path, fake_source):
    raw, db = tmp_path / "raw", tmp_path / "w.duckdb"
    extract_day(fake_source, DT, raw, max_pages=5, roles=ROLES)
    load_day(db, raw, DT)
    first = counts(db)
    load_day(db, raw, DT)
    load_day(db, raw, DT)
    assert counts(db) == first


def test_loading_another_day_keeps_existing_days(tmp_path, fake_source):
    raw, db = tmp_path / "raw", tmp_path / "w.duckdb"
    for d in (DT, date(2026, 11, 3)):
        extract_day(fake_source, d, raw, max_pages=5, roles=ROLES)
        load_day(db, raw, d)
    with duckdb.connect(str(db), read_only=True) as con:
        per_day = con.execute(
            "select snapshot_date, count(*) from raw.adzuna_postings group by 1 order by 1"
        ).fetchall()
    assert [n for _, n in per_day] == [5, 5]


def test_missing_partition_raises_clear_error(tmp_path):
    with pytest.raises(FileNotFoundError, match="Run extract first"):
        load_day(tmp_path / "w.duckdb", tmp_path / "raw", DT)
