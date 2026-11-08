"""Dashboard queries and the Streamlit app, on a real warehouse built from sample data
(extract -> load -> dbt build), so the SQL is tested against the actual mart schema."""

from __future__ import annotations

import os
import shutil
from datetime import date

import pytest

pd = pytest.importorskip("pandas")
if not (os.getenv("DBT_EXECUTABLE") or shutil.which("dbt")):
    pytest.skip("dbt is not installed", allow_module_level=True)

from dashboard import queries as q  # noqa: E402


@pytest.fixture(scope="module")
def warehouse(tmp_path_factory):
    from ingestion.pipeline import main

    root = tmp_path_factory.mktemp("dash")
    env = {"DATA_DIR": str(root), "DUCKDB_PATH": str(root / "w.duckdb"), "STORAGE_BACKEND": "local"}
    with pytest.MonkeyPatch.context() as mp:
        for k, v in env.items():
            mp.setenv(k, v)
        for d in (date(2026, 11, 2), date(2026, 11, 3)):
            assert main(["--source", "sample", "--date", d.isoformat()]) == 0
        yield root


@pytest.fixture
def con(warehouse):
    with q.connect(warehouse / "w.duckdb") as c:
        yield c


def test_overview_counts_each_posting_once(con):
    ov = q.overview(con)
    assert ov["postings"] == con.execute("select count(*) from marts.fct_postings").fetchone()[0]
    assert ov["days_loaded"] == 2
    assert 0 < ov["share_with_salary"] <= 1


def test_top_skills_per_role_and_all_roles(con):
    week = q.weeks(con)[0]
    one = q.top_skills(con, week, "data_engineer")
    assert not one.empty and one["share"].between(0, 1).all()
    assert one["postings_with_skill"].is_monotonic_decreasing

    everyone = q.top_skills(con, week, None, limit=100)
    total = con.execute(
        "select count(*) from marts.fct_postings where date_trunc('week', first_seen_date) = ?",
        [week],
    ).fetchone()[0]
    # The all-roles share is over all of the week's postings, not an average of roles.
    sql = everyone.loc[everyone["skill_name"] == "SQL"].iloc[0]
    assert sql["role_postings"] == total
    assert sql["share"] == pytest.approx(sql["postings_with_skill"] / total)


def test_salary_quartiles_are_ordered(con):
    with_est = q.salary_by_role(con, include_predicted=True)
    without = q.salary_by_role(con, include_predicted=False)
    assert ((with_est["p25"] <= with_est["median"]) & (with_est["median"] <= with_est["p75"])).all()
    assert without["postings"].sum() < with_est["postings"].sum()


def test_top_companies_role_filter(con):
    all_roles = q.top_companies(con, None, limit=100)
    analysts = q.top_companies(con, "data_analyst", limit=100)
    assert analysts["postings"].sum() < all_roles["postings"].sum()
    assert "Unknown" not in set(all_roles["company_name"])


def test_reposts_reads_spark_output(con, tmp_path):
    assert q.reposts(con, tmp_path) is None  # Spark job not run yet
    out = tmp_path / "spark"
    out.mkdir()
    con.execute(
        f"""copy (select * from (values ('a', 'a', 'Acme', false), ('b', 'a', 'Acme', true),
                                        ('c', 'c', null, false))
                  t(posting_id, canonical_posting_id, company_name, is_repost))
            to '{out}/part-0.parquet' (format parquet)"""
    )
    rp = q.reposts(con, out)
    assert (rp["postings"], rp["distinct_jobs"], rp["reposts"]) == (3, 2, 1)
    assert rp["by_company"].to_dict("records") == [{"company_name": "Acme", "reposts": 1}]


def test_pipeline_health(con):
    assert q.latest_dbt_results(con).get("pass", 0) > 0
    runs = q.pipeline_runs(con)
    assert {"extract", "load", "dbt_build", "total"} <= set(runs["step"])


def test_app_renders_and_filters(warehouse, monkeypatch):
    from streamlit.testing.v1 import AppTest

    monkeypatch.setenv("DATA_DIR", str(warehouse))
    monkeypatch.setenv("DUCKDB_PATH", str(warehouse / "w.duckdb"))
    at = AppTest.from_file("../dashboard/app.py", default_timeout=60).run()
    assert not at.exception
    assert at.metric[0].value == str(q.overview(q.connect(warehouse / "w.duckdb"))["postings"])

    at.selectbox[0].select("data_analyst").run()  # role filter
    at.toggle[0].set_value(False).run()  # leave out Adzuna's estimated salaries
    assert not at.exception
