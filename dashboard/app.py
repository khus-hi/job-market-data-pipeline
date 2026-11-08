"""Streamlit dashboard over the dbt marts.

    streamlit run dashboard/app.py

Reads the DuckDB warehouse named by DUCKDB_PATH (default data/warehouse.duckdb), read-only.
All SQL lives in dashboard/queries.py.
"""

from __future__ import annotations

import sys
from pathlib import Path

import altair as alt
import streamlit as st

sys.path.insert(
    0, str(Path(__file__).resolve().parent.parent)
)  # so `streamlit run` finds ingestion/

from dashboard import queries as q  # noqa: E402
from ingestion.config import get_settings  # noqa: E402

BAR = "#2a78d6"  # one series per chart, so one hue
ROLE_LABELS = {
    "data_engineer": "Data engineer",
    "data_analyst": "Data analyst",
    "analytics_engineer": "Analytics engineer",
    "data_scientist": "Data scientist",
    "other": "Other",
}

st.set_page_config(page_title="Job Market Dashboard", layout="wide")
settings = get_settings()


@st.cache_data(ttl=300)  # re-query at most every 5 minutes; the data changes daily
def load(name: str, *args):
    with q.connect(settings.duckdb_path) as con:
        return getattr(q, name)(con, *args)


def bars(df, x: str, y: str, x_title: str, fmt: str, tooltip: list) -> alt.Chart:
    """Horizontal bars, sorted, thin, with rounded ends and a hover tooltip."""
    return (
        alt.Chart(df)
        .mark_bar(color=BAR, cornerRadiusEnd=4, height={"band": 0.6})
        .encode(
            x=alt.X(x, title=x_title, axis=alt.Axis(format=fmt, grid=True, tickCount=5)),
            y=alt.Y(y, sort="-x", title=None, axis=alt.Axis(labelOverlap=False, labelLimit=180)),
            tooltip=tooltip,
        )
        .properties(height=60 + 32 * len(df))
    )


st.title("Job market for data roles")

try:
    ov = load("overview")
except FileNotFoundError as exc:
    st.error(str(exc))
    st.stop()

if not ov["postings"]:
    st.warning("The warehouse has no postings yet. Run `python -m ingestion.pipeline` first.")
    st.stop()

st.caption(
    f"US postings from the Adzuna API, {ov['first_day']} to {ov['last_day']} "
    f"({ov['days_loaded']} day(s) of snapshots). A posting counts once, however many days "
    "it stays live."
)

c1, c2, c3, c4 = st.columns(4)
c1.metric("Unique postings", f"{ov['postings']:,}")
c2.metric("Companies", f"{ov['companies']:,}")
c3.metric("Days of data", ov["days_loaded"])
c4.metric("Postings with a salary", f"{ov['share_with_salary']:.0%}")

# Filters, in one row above the charts they control.
weeks = load("weeks")
f1, f2, _ = st.columns([1, 1, 2])
role = f1.selectbox(
    "Role",
    [None, *q.ROLES],
    format_func=lambda r: "All roles" if r is None else ROLE_LABELS[r],
)
week = f2.selectbox("Week starting", weeks, format_func=lambda d: d.strftime("%b %d, %Y"))
role_name = "all roles" if role is None else ROLE_LABELS[role].lower()

left, right = st.columns(2)

with left:
    st.subheader("Most requested skills")
    skills = load("top_skills", week, role)
    if skills.empty:
        st.info("No postings for this role in this week.")
    else:
        n = int(skills["role_postings"].iloc[0])
        st.caption(f"Share of {n:,} new postings for {role_name} that mention each skill.")
        st.altair_chart(
            bars(
                skills, "share:Q", "skill_name:N", "Share of postings", ".0%",
                [alt.Tooltip("skill_name:N", title="Skill"),
                 alt.Tooltip("category:N", title="Category"),
                 alt.Tooltip("postings_with_skill:Q", title="Postings"),
                 alt.Tooltip("share:Q", title="Share", format=".1%")],
            ),
            width="stretch",
        )  # fmt: skip

with right:
    st.subheader("Top hiring companies")
    companies = load("top_companies", role)
    st.caption(f"Unique postings for {role_name}, all weeks.")
    st.altair_chart(
        bars(
            companies, "postings:Q", "company_name:N", "Postings", "d",
            [alt.Tooltip("company_name:N", title="Company"),
             alt.Tooltip("postings:Q", title="Postings")],
        ),
        width="stretch",
    )  # fmt: skip

left, right = st.columns(2)

with left:
    st.subheader("Salary by role")
    include_predicted = st.toggle(
        "Include salaries estimated by Adzuna",
        value=True,
        help="Adzuna fills in an estimated salary when the employer gives none.",
    )
    sal = load("salary_by_role", include_predicted)
    if sal.empty:
        st.info("No salaries in the data.")
    else:
        sal["role_label"] = sal["role"].map(ROLE_LABELS)
        st.caption("Line = middle 50% of salary midpoints (25th to 75th percentile), dot = median.")
        base = alt.Chart(sal).encode(
            y=alt.Y(
                "role_label:N",
                sort=alt.SortField("median", order="descending"),
                title=None,
                axis=alt.Axis(labelLimit=180),
            ),
            tooltip=[
                alt.Tooltip("role_label:N", title="Role"),
                alt.Tooltip("postings:Q", title="Postings with salary"),
                alt.Tooltip("p25:Q", title="25th percentile", format="$,.0f"),
                alt.Tooltip("median:Q", title="Median", format="$,.0f"),
                alt.Tooltip("p75:Q", title="75th percentile", format="$,.0f"),
            ],
        )
        x = alt.X(
            "p25:Q",
            title="Annual salary (USD)",
            scale=alt.Scale(zero=False, nice=True),
            axis=alt.Axis(format="$~s", tickCount=6),
        )
        chart = base.mark_rule(color=BAR, strokeWidth=2).encode(x=x, x2="p75:Q") + base.mark_point(
            color=BAR, filled=True, size=90
        ).encode(x="median:Q")
        st.altair_chart(chart.properties(height=max(160, 48 * len(sal))), width="stretch")

with right:
    st.subheader("Postings by role")
    by_role = load("postings_by_role")
    by_role["role_label"] = by_role["role"].map(ROLE_LABELS)
    st.caption("Role is classified from the job title, not the search term.")
    st.altair_chart(
        bars(
            by_role, "postings:Q", "role_label:N", "Postings", "d",
            [alt.Tooltip("role_label:N", title="Role"),
             alt.Tooltip("postings:Q", title="Postings")],
        ),
        width="stretch",
    )  # fmt: skip

st.subheader("Reposts")
rp = load("reposts", settings.data_dir / "spark" / "postings_dedup")
if rp is None:
    st.info("Run `python -m spark.dedup_postings` to see reposts (needs Java, see docs/spark.md).")
else:
    r1, r2, r3 = st.columns(3)
    r1.metric("Unique posting ids", f"{rp['postings']:,}")
    r2.metric("Distinct jobs", f"{rp['distinct_jobs']:,}")
    r3.metric("Reposts", f"{rp['reposts']:,}")
    st.caption(
        "From the Spark job: the same company, title and location re-listed under a new id "
        "within 14 days counts as one job."
    )
    if not rp["by_company"].empty:
        st.dataframe(
            rp["by_company"].rename(columns={"company_name": "Company", "reposts": "Reposts"}),
            hide_index=True,
        )

with st.expander("Pipeline health"):
    dbt = load("latest_dbt_results")
    st.write(
        "Latest dbt build: " + ", ".join(f"{n} {status}" for status, n in sorted(dbt.items()))
        if dbt
        else "No dbt results logged yet."
    )
    st.dataframe(load("pipeline_runs"), hide_index=True, width="stretch")

with st.expander("Data tables"):
    st.write("Skills chart data")
    st.dataframe(skills, hide_index=True, width="stretch")
    st.write("Salary chart data")
    st.dataframe(sal, hide_index=True, width="stretch")
