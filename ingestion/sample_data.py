"""Deterministic FAKE postings shaped like Adzuna responses.

Use it to develop and test without API keys (`--source sample`). Every company
is invented and every ID starts with "sample-". Never quote numbers from sample
runs as real results (résumé metrics must come from real Adzuna runs).

Each fake posting is "live" for a window of days, so the same posting appears
on several consecutive days, just like the real API. That gives the
deduplication logic something real to do.
"""

from __future__ import annotations

import random
from datetime import date, timedelta
from typing import Any

COMPANIES = [
    "Acme Analytics",
    "Globex Data Co",
    "Initech",
    "Umbrella Insights",
    "Stark Metrics",
    "Wayne Logistics",
    "Hooli",
    "Pied Piper",
    "Vandelay Industries",
    "Soylent Foods",
    "Cyberdyne Cloud",
    "Wonka Retail",
]
CITIES = ["New York, NY", "Austin, TX", "Seattle, WA", "Chicago, IL", "Remote", "Denver, CO"]
TITLES = {
    "data engineer": ["Data Engineer", "Senior Data Engineer", "Data Engineer II"],
    "data analyst": ["Data Analyst", "Business Intelligence Analyst", "Senior Data Analyst"],
    "analytics engineer": ["Analytics Engineer", "Senior Analytics Engineer"],
    "data scientist": ["Data Scientist", "Machine Learning Data Scientist"],
}
SKILLS = {
    "data engineer": [
        "Python",
        "SQL",
        "Airflow",
        "Spark",
        "AWS",
        "Kafka",
        "dbt",
        "Snowflake",
        "Docker",
        "Terraform",
    ],
    "data analyst": ["SQL", "Excel", "Tableau", "Power BI", "Python", "Looker", "statistics"],
    "analytics engineer": ["SQL", "dbt", "Snowflake", "BigQuery", "Looker", "Git", "Python"],
    "data scientist": [
        "Python",
        "SQL",
        "machine learning",
        "pandas",
        "statistics",
        "R",
        "Spark",
        "AWS",
    ],
}
SALARY_BANDS = {
    "data engineer": (110_000, 175_000),
    "data analyst": (65_000, 110_000),
    "analytics engineer": (100_000, 160_000),
    "data scientist": (115_000, 185_000),
}
POOL_START = date(2026, 1, 1)
POOL_PER_ROLE = 2000


def _posting(role: str, n: int) -> tuple[date, date, dict[str, Any]]:
    rng = random.Random(f"{role}-{n}")  # same seed -> same posting every time
    opened = POOL_START + timedelta(days=rng.randint(0, 400))
    closed = opened + timedelta(days=rng.randint(2, 9))
    skills = rng.sample(SKILLS[role], k=rng.randint(2, 5))
    lo, hi = SALARY_BANDS[role]
    smin = rng.randrange(lo, hi, 5_000)
    has_salary = rng.random() > 0.25
    city = rng.choice(CITIES)
    posting = {
        "id": f"sample-{role.replace(' ', '')}-{n:04d}",
        "title": rng.choice(TITLES[role]),
        "description": (
            f"We are hiring! You will build reliable data products using {', '.join(skills)}. "
            "Strong communication skills and a growth mindset are a plus…"
        ),
        "created": f"{opened.isoformat()}T{rng.randint(0, 23):02d}:00:00Z",
        "redirect_url": f"https://example.com/jobs/{role.replace(' ', '-')}-{n}",
        "company": {"display_name": rng.choice(COMPANIES)},
        "location": {"display_name": city, "area": ["US", city]},
        "category": {"label": "IT Jobs", "tag": "it-jobs"},
        "contract_time": rng.choice(["full_time", "full_time", "part_time", None]),
        "contract_type": rng.choice(["permanent", "permanent", "contract", None]),
        "salary_min": smin if has_salary else None,
        "salary_max": smin + rng.randrange(10_000, 40_000, 5_000) if has_salary else None,
        "salary_is_predicted": rng.choice(["0", "1"]) if has_salary else "0",
        "__CLASS__": "Adzuna::API::Response::Job",
    }
    return opened, closed, posting


class SamplePostingSource:
    """Mimics AdzunaClient.search() for a given snapshot date."""

    def __init__(self, snapshot_date: date, results_per_page: int = 50) -> None:
        self.snapshot_date = snapshot_date
        self.results_per_page = results_per_page

    def _live(self, role: str) -> list[dict[str, Any]]:
        live = []
        for n in range(POOL_PER_ROLE):
            opened, closed, posting = _posting(role, n)
            if opened <= self.snapshot_date <= closed:
                live.append(posting)
        return live

    def search(self, role: str, page: int) -> dict[str, Any]:
        live = self._live(role)
        start = (page - 1) * self.results_per_page
        return {
            "count": len(live),
            "mean": None,
            "results": live[start : start + self.results_per_page],
            "__CLASS__": "Adzuna::API::Response::JobSearchResults",
        }
