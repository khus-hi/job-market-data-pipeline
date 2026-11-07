import pytest

from ingestion.skills import clean_text, extract_skills, load_catalog


def test_catalog_loads_and_names_are_unique():
    names = [s.name for s in load_catalog()]
    assert len(names) == len(set(names))
    assert {"SQL", "Python", "Airflow", "dbt", "Spark"} <= set(names)


@pytest.mark.parametrize(
    "text, expected",
    [
        ("Strong SQL and Python skills", {"SQL", "Python"}),
        ("Experience with PySpark and Apache Airflow", {"Spark", "Airflow"}),
        ("dbt, Snowflake, BigQuery", {"dbt", "Snowflake", "BigQuery"}),
        ("Power BI or PowerBI dashboards", {"Power BI"}),
        ("Kubernetes (k8s) and Docker", {"Kubernetes", "Docker"}),
    ],
)
def test_finds_expected_skills(text, expected):
    assert set(extract_skills(text)) == expected


@pytest.mark.parametrize(
    "text, not_expected",
    [
        ("Experience with NoSQL databases", "SQL"),  # word boundary
        ("JavaScript and TypeScript", "Java"),  # Java != JavaScript
        ("You will excel at communication", "Excel"),  # verb, not the tool
        ("Our R&D team", "R"),  # R&D is not the R language
        ("Microsoft SQL Server", "Python"),
    ],
)
def test_avoids_false_positives(text, not_expected):
    assert not_expected not in extract_skills(text)


def test_r_language_is_detected_when_standalone():
    assert "R" in extract_skills("Statistical modeling in R or Python")


def test_case_insensitive_and_deduplicated():
    assert extract_skills("sql SQL Sql") == ["SQL"]


def test_combines_title_and_description_and_handles_none():
    assert extract_skills("Spark Engineer", None) == ["Spark"]
    assert extract_skills(None, None) == []


def test_clean_text_strips_html():
    assert clean_text("<p>SQL&amp;Python</p>\n\n<b>dbt</b>") == "SQL&Python dbt"
    assert extract_skills("<li>Airflow</li><li>dbt</li>") == ["Airflow", "dbt"]
