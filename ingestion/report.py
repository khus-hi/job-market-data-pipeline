"""Print the "top skills this week" answer straight from the mart."""

from __future__ import annotations

import argparse

import duckdb

from ingestion.config import REPO_ROOT, get_settings

QUERY_PATH = REPO_ROOT / "dbt" / "analyses" / "top_skills_this_week.sql"


def top_skills(db_path, limit: int = 10) -> list[tuple]:
    sql = QUERY_PATH.read_text(encoding="utf-8")
    with duckdb.connect(str(db_path), read_only=True) as con:
        return con.execute(f"select * from ({sql}) limit {int(limit)}").fetchall()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--limit", type=int, default=10)
    args = parser.parse_args()
    rows = top_skills(get_settings().duckdb_path, args.limit)
    print(f"{'week':<12}{'skill':<20}{'postings':>9}{'share':>8}")
    for week, skill, n, share in rows:
        print(f"{str(week):<12}{skill:<20}{n:>9}{share:>8.1%}")


if __name__ == "__main__":
    main()
