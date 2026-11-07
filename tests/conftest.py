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
