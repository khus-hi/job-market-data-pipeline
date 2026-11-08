"""SQL behind the dashboard. Plain functions over a DuckDB connection, so they're
unit-tested without Streamlit and the app file only does layout.

Everything reads the dbt marts (and the meta schema). Nothing here writes.
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd

ROLES = ["data_engineer", "data_analyst", "analytics_engineer", "data_scientist", "other"]


def connect(db_path: Path) -> duckdb.DuckDBPyConnection:
    """Read-only, so the dashboard can never change the warehouse.

    DuckDB allows one writing process at a time, so the app opens a short-lived
    connection per refresh instead of holding one open while the pipeline runs.
    """
    if not db_path.exists():
        raise FileNotFoundError(f"No warehouse at {db_path}. Run the pipeline first.")
    return duckdb.connect(str(db_path), read_only=True)


def overview(con) -> dict:
    row = con.execute(
        """
        select
            count(*)                                         as postings,
            count(distinct company_id)                       as companies,
            min(first_seen_date)                             as first_day,
            max(last_seen_date)                              as last_day,
            avg(case when salary_mid is not null then 1 else 0 end) as share_with_salary,
            (select count(distinct snapshot_date) from raw.adzuna_postings) as days_loaded
        from marts.fct_postings
        """
    ).fetchone()
    keys = ["postings", "companies", "first_day", "last_day", "share_with_salary", "days_loaded"]
    return dict(zip(keys, row, strict=True))


def weeks(con) -> list:
    """Weeks with data, newest first (a week = new postings by first_seen_date)."""
    rows = con.execute(
        "select distinct week_start from marts.mart_top_skills_by_role_week order by 1 desc"
    ).fetchall()
    return [r[0] for r in rows]


def top_skills(con, week, role: str | None, limit: int = 15) -> pd.DataFrame:
    """Share of the week's new postings that mention each skill.

    role=None means all roles: the counts are re-aggregated from the mart, and the
    share is over all postings that week, not an average of per-role shares.
    """
    if role:
        return con.execute(
            """
            select skill_name, category, postings_with_skill, role_postings,
                   share_of_role_postings as share
            from marts.mart_top_skills_by_role_week
            where week_start = ? and role = ?
            order by postings_with_skill desc, skill_name
            limit ?
            """,
            [week, role, limit],
        ).df()
    return con.execute(
        """
        with totals as (
            select sum(role_postings) as postings
            from (select distinct role, role_postings
                  from marts.mart_top_skills_by_role_week where week_start = $week)
        )
        select skill_name, any_value(category) as category,
               sum(postings_with_skill) as postings_with_skill,
               any_value(totals.postings) as role_postings,
               sum(postings_with_skill) / any_value(totals.postings) as share
        from marts.mart_top_skills_by_role_week, totals
        where week_start = $week
        group by skill_name
        order by postings_with_skill desc, skill_name
        limit $limit
        """,
        {"week": week, "limit": limit},
    ).df()


def salary_by_role(con, include_predicted: bool) -> pd.DataFrame:
    """Quartiles of the salary midpoint per role. Adzuna estimates some salaries
    (is_salary_predicted); they can be left out."""
    return con.execute(
        """
        select role,
               count(*)                                  as postings,
               quantile_cont(salary_mid, 0.25)           as p25,
               median(salary_mid)                        as median,
               quantile_cont(salary_mid, 0.75)           as p75
        from marts.fct_postings
        where salary_mid is not null and (? or not is_salary_predicted)
        group by role
        order by median desc
        """,
        [include_predicted],
    ).df()


def postings_by_role(con) -> pd.DataFrame:
    return con.execute(
        "select role, count(*) as postings from marts.fct_postings group by role order by 2 desc"
    ).df()


def top_companies(con, role: str | None, limit: int = 10) -> pd.DataFrame:
    return con.execute(
        """
        select c.company_name, count(*) as postings
        from marts.fct_postings as f
        join marts.dim_company as c using (company_id)
        where (? is null or f.role = ?) and c.company_name <> 'Unknown'
        group by c.company_name
        order by postings desc, c.company_name
        limit ?
        """,
        [role, role, limit],
    ).df()


def reposts(con, parquet_dir: Path) -> dict | None:
    """Summary of the Spark job's output, or None if it hasn't been run."""
    if not any(parquet_dir.glob("*.parquet")):
        return None
    files = str(parquet_dir / "*.parquet")
    summary = con.execute(
        f"""
        select count(*) as postings,
               count(distinct canonical_posting_id) as distinct_jobs,
               count(*) filter (where is_repost) as reposts
        from read_parquet('{files}')
        """
    ).fetchone()
    by_company = con.execute(
        f"""
        select company_name, count(*) filter (where is_repost) as reposts
        from read_parquet('{files}')
        where company_name is not null
        group by company_name
        having reposts > 0
        order by reposts desc, company_name
        limit 10
        """
    ).df()
    return {
        "postings": summary[0],
        "distinct_jobs": summary[1],
        "reposts": summary[2],
        "by_company": by_company,
    }


def pipeline_runs(con, limit: int = 20) -> pd.DataFrame:
    return con.execute(
        """
        select timezone('UTC', logged_at) as logged_at_utc, snapshot_date, step, status,
               row_count, round(seconds, 1) as seconds
        from meta.pipeline_runs
        order by logged_at desc
        limit ?
        """,
        [limit],
    ).df()


def latest_dbt_results(con) -> dict:
    """Pass/fail counts from the most recent dbt build."""
    rows = con.execute(
        """
        select status, count(*)
        from meta.dbt_results
        where run_id = (select run_id from meta.dbt_results order by logged_at desc limit 1)
        group by status
        """
    ).fetchall()
    return dict(rows)
