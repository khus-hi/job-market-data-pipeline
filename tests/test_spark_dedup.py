"""Spark dedup job. Skipped when PySpark or Java isn't installed (pip install -r
requirements-spark.txt, plus a Java 17+ runtime)."""

from __future__ import annotations

import shutil
from datetime import date

import pytest

pytest.importorskip("pyspark")
if not shutil.which("java"):
    pytest.skip("Java is not installed", allow_module_level=True)

from ingestion.extract import extract_day  # noqa: E402
from ingestion.storage import LocalStore  # noqa: E402
from spark.dedup_postings import build_spark, run_job  # noqa: E402
from tests.conftest import FakeSource, make_posting  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    session = build_spark("test")
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def job(company: str, pid: str, title: str = "Data Engineer", city: str = "Austin, TX", **kw):
    return make_posting(
        pid, title=title, company={"display_name": company},
        location={"display_name": city, "area": ["US", "Texas"]}, **kw,
    )  # fmt: skip


def land(store: LocalStore, dt: date, pages: dict[str, list[list[dict]]]) -> None:
    extract_day(FakeSource(pages, results_per_page=50), dt, store, max_pages=1, roles=list(pages))


def run(spark, store: LocalStore, out, gap_days: int = 14):
    dirs = [store.describe(d) for d in store.list_partitions()]
    stats = run_job(spark, dirs, out, gap_days)
    rows = {r["posting_id"]: r.asDict() for r in spark.read.parquet(str(out)).collect()}
    return stats, rows


def test_one_row_per_posting_with_latest_attributes(spark, tmp_path):
    store = LocalStore(tmp_path / "lake")
    land(store, date(2026, 11, 1), {
        "data engineer": [[job("Acme", "1", salary_min=100000)]],
        "data analyst": [[job("Acme", "1", salary_min=100000)]],  # same posting, 2nd search
    })  # fmt: skip
    land(store, date(2026, 11, 3), {"data engineer": [[job("Acme", "1", salary_min=120000)]]})

    stats, rows = run(spark, store, tmp_path / "out")

    assert stats["sightings"] == 3 and stats["postings"] == 1
    p = rows["1"]
    assert (p["first_seen_date"], p["last_seen_date"]) == (date(2026, 11, 1), date(2026, 11, 3))
    assert p["days_seen"] == 2 and p["times_returned"] == 3
    assert p["salary_min"] == 120000  # attributes come from the newest sighting


def test_reposts_grouped_only_within_gap(spark, tmp_path):
    store = LocalStore(tmp_path / "lake")
    land(store, date(2026, 11, 1), {"data engineer": [[
        job("Acme", "a1", title="Sr. Data Engineer"),
        job("Globex", "g1"),
        make_posting("n1", company={"display_name": ""}),  # no company: never matched
    ]]})  # fmt: skip
    land(store, date(2026, 11, 5), {"data engineer": [[
        job("ACME", "a2", title="sr data engineer"),       # repost: same job, new id
        job("Globex", "g2", city="Remote"),                # different location: new job
        make_posting("n2", company={"display_name": ""}),
    ]]})  # fmt: skip
    land(store, date(2026, 11, 30), {"data engineer": [[
        job("Acme", "a3", title="Sr Data Engineer"),       # 25 days later: a new opening
    ]]})  # fmt: skip

    stats, rows = run(spark, store, tmp_path / "out", gap_days=14)

    assert rows["a2"]["canonical_posting_id"] == "a1" and rows["a2"]["is_repost"]
    assert rows["a1"]["postings_in_group"] == 2
    assert rows["a3"]["canonical_posting_id"] == "a3" and not rows["a3"]["is_repost"]
    assert rows["g2"]["canonical_posting_id"] == "g2"
    assert rows["n2"]["canonical_posting_id"] == "n2"
    per_day = stats.pop("sightings_per_day")
    assert stats == {"days": 3, "sightings": 7, "postings": 7, "distinct_jobs": 6, "reposts": 1}
    assert per_day == {"2026-11-01": 3, "2026-11-05": 3, "2026-11-30": 1}


def test_rerun_overwrites_output(spark, tmp_path):
    store = LocalStore(tmp_path / "lake")
    land(store, date(2026, 11, 1), {"data engineer": [[job("Acme", "1"), job("Acme", "2")]]})
    out = tmp_path / "out"
    first, _ = run(spark, store, out)
    second, rows = run(spark, store, out)
    assert first == second and len(rows) == 2


def test_s3_landing_zone_is_mirrored_locally(store, tmp_path, monkeypatch):
    """With S3 as the landing zone, Spark reads a local copy of every landed day."""
    from ingestion.config import Settings
    from ingestion.storage import S3Store
    from spark import dedup_postings

    land(store, date(2026, 11, 1), {"data engineer": [[job("Acme", "1")]]})
    land(store, date(2026, 11, 2), {"data engineer": [[job("Acme", "1")]]})
    monkeypatch.setattr("ingestion.config.get_store", lambda s: store)
    s = Settings(None, None, tmp_path, tmp_path / "w.duckdb", 1, 1)

    dirs = dedup_postings.local_partitions(s, tmp_path / "cache")

    names = [d.rsplit("/", 1)[-1] for d in dirs]
    assert names == ["dt=2026-11-01", "dt=2026-11-02"]
    expected_root = tmp_path / "cache" if isinstance(store, S3Store) else tmp_path / "lake"
    assert all(d.startswith(str(expected_root)) for d in dirs)


def test_missing_or_short_fields_do_not_fail_the_job(spark, tmp_path):
    """Real Adzuna postings can have area=["US"], no location, or an odd date. Spark 4 runs
    in ANSI mode, where an out-of-range index or a bad timestamp fails the whole job."""
    store = LocalStore(tmp_path / "lake")
    land(store, date(2026, 11, 1), {"data engineer": [[
        make_posting("1", location={"display_name": "US", "area": ["US"]}),
        make_posting("2", created="not a date"),
        {"id": 3, "title": "Data Engineer"},  # numeric id, no company, no location
    ]]})  # fmt: skip

    stats, rows = run(spark, store, tmp_path / "out")

    assert stats["postings"] == 3
    assert rows["1"]["state_name"] is None
    assert rows["2"]["posted_at"] is None
    assert rows["3"]["location_name"] is None


def test_days_out_of_sync():
    from spark.dedup_postings import days_out_of_sync

    landed = {"2026-11-07": 573, "2026-11-08": 553, "2026-11-09": 10}
    loaded = {"2026-11-07": 567, "2026-11-08": 553}  # 07 re-extracted, 09 never loaded
    assert days_out_of_sync(landed, loaded) == ["2026-11-07", "2026-11-09"]
