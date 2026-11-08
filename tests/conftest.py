"""Shared test helpers. Tests never call the real API and never touch data/."""

from __future__ import annotations

from typing import Any

import pytest


def make_posting(
    pid: str, title: str = "Data Engineer", description: str = "SQL and Python", **extra: Any
) -> dict[str, Any]:
    return {
        "id": pid,
        "title": title,
        "description": description,
        "company": {"display_name": "Test Co"},
        **extra,
    }


class FakeSource:
    """Serves canned pages: {role: [page1_results, page2_results, ...]}."""

    def __init__(self, pages: dict[str, list[list[dict]]], results_per_page: int = 2) -> None:
        self.pages = pages
        self.results_per_page = results_per_page
        self.calls: list[tuple[str, int]] = []

    def search(self, role: str, page: int) -> dict[str, Any]:
        self.calls.append((role, page))
        role_pages = self.pages.get(role, [])
        results = role_pages[page - 1] if page <= len(role_pages) else []
        return {"count": sum(len(p) for p in role_pages), "results": results}


@pytest.fixture
def fake_source() -> FakeSource:
    return FakeSource(
        {
            "data engineer": [
                [make_posting("1"), make_posting("2", description="Airflow, dbt, Spark")],
                [make_posting("3", description="Kafka")],  # short page -> last page
            ],
            "data analyst": [
                [
                    make_posting("4", title="Data Analyst", description="Tableau and Excel"),
                    make_posting("1"),
                ],  # same posting returned by a second search
            ],
        }
    )


@pytest.fixture(params=["local", "s3"])
def store(request, tmp_path, monkeypatch):
    """Every test that uses `store` runs twice: once on a local folder, once on a
    mocked S3 bucket (moto). That proves both backends behave the same."""
    from ingestion.storage import LocalStore, S3Store

    if request.param == "local":
        yield LocalStore(tmp_path / "lake")
        return

    import boto3
    from moto import mock_aws

    for var in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"):
        monkeypatch.setenv(var, "testing")  # fake creds: moto never talks to AWS
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket="test-bucket")
        yield S3Store("test-bucket", client)


@pytest.fixture(scope="session")
def sample_warehouse(tmp_path_factory):
    """A real warehouse: two sample days through extract -> load -> dbt build.
    Shared by tests that need the actual mart schema. Skips when dbt isn't installed."""
    import os
    import shutil
    from datetime import date

    if not (os.getenv("DBT_EXECUTABLE") or shutil.which("dbt")):
        pytest.skip("dbt is not installed")
    from ingestion.pipeline import main

    root = tmp_path_factory.mktemp("warehouse")
    env = {"DATA_DIR": str(root), "DUCKDB_PATH": str(root / "w.duckdb"), "STORAGE_BACKEND": "local"}
    with pytest.MonkeyPatch.context() as mp:
        for k, v in env.items():
            mp.setenv(k, v)
        for d in (date(2026, 11, 2), date(2026, 11, 3)):
            assert main(["--source", "sample", "--date", d.isoformat()]) == 0
    return root
