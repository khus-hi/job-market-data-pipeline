# Spark: cross-day deduplication

`spark/dedup_postings.py` reads every day in the raw landing zone and removes two kinds of
duplicates:

| Level | Problem | Rule | Output columns |
|---|---|---|---|
| 1. Same posting | A live posting comes back every day it's open, often under several role searches | One row per Adzuna id, attributes from the newest sighting | `first_seen_date`, `last_seen_date`, `days_seen`, `times_returned` |
| 2. Reposts | An employer closes a posting and re-lists the same job under a new id | Same company, normalised title and location, first seen within 14 days (`--repost-gap-days`) of an earlier one in the group ending | `canonical_posting_id`, `is_repost`, `postings_in_group` |

Output is Parquet at `data/spark/postings_dedup/`, overwritten on every run, and each run logs a
`spark_dedup` row to `meta.pipeline_runs`.

Level 2 is *sessionisation*: within each (company, title, location) key, sorted by first seen, a
running max of `last_seen_date` over the earlier rows tells whether a posting continues the
current group or starts a new one, and a running sum of the "starts a group" flags numbers the
groups. Two postings for the same job that are live at the same time (several openings) also
count as one job.

Level 1 is the same logic as dbt's `fct_postings`. `--check-dbt` compares the two and fails if
any posting's id, first seen or last seen date differ.

## Setup (once, on a Mac)

PySpark runs on the JVM, so it needs Java:

```bash
brew install openjdk@21
echo 'export JAVA_HOME="$(brew --prefix openjdk@21)/libexec/openjdk.jdk/Contents/Home"' >> ~/.zprofile
echo 'export PATH="$JAVA_HOME/bin:$PATH"' >> ~/.zprofile
source ~/.zprofile && java -version          # should print 21.x
source .venv/bin/activate
pip install -r requirements-spark.txt
```

## Run

```bash
python -m spark.dedup_postings --check-dbt
```

It reads the landing zone set by `STORAGE_BACKEND`. With S3, the days are first copied into
`data/spark/raw_cache/` and Spark reads them from there. Reading `s3a://` directly would need the
`hadoop-aws` jars and the AWS SDK bundle, several hundred MB of downloads to read a few MB of
JSON. On a real cluster you would read `s3a://` directly.

`--check-dbt` first checks that the warehouse holds the same version of every landed day (same
raw row count per day). If a day was never loaded, or was re-extracted after loading (an Airflow
run lands into the same bucket but loads its own warehouse file), it lists those days and the
command that reloads each one, instead of reporting a misleading difference.

Query the output from DuckDB, no Spark needed:

```sql
select count(*) as postings, count(distinct canonical_posting_id) as jobs
from 'data/spark/postings_dedup/*.parquet';
```

## Spark or SQL?

For this project's data, SQL is the better tool. One day is a few hundred postings, a year is
well under a million rows, and DuckDB on a laptop handles that in under a second. The Spark job
took about 14 seconds on five days of test data, most of it starting the JVM and Spark. That's
why dbt and DuckDB stay the main transformation path, and the Spark job exists to show the same
logic in the tool you'd use at a scale where it matters.

Spark is worth it when:
- **The data doesn't fit on one machine.** Hundreds of GB to TB per run, where a single-node
  engine runs out of memory or takes hours.
- **The input is raw files in a lake, not warehouse tables.** Spark reads JSON or Parquet straight
  from S3 in parallel, without loading it anywhere first.
- **The logic isn't comfortable in SQL**, such as complex parsing, Python libraries (NLP for
  skill extraction, fuzzy matching), or ML feature pipelines.
- **A cluster already exists** (EMR, Databricks, Glue), so the startup cost is shared.

SQL in the warehouse is better when:
- the data is already in the warehouse and fits its engine (most analytics work)
- the people maintaining it are analysts and analytics engineers who know SQL and dbt
- you want dbt's tests, lineage and docs on every step
- the overhead of a cluster (cost, startup, ops) is larger than the job itself

Rule of thumb: start in SQL, and move a step to Spark when it stops fitting on one machine, not
before.

## Tests

`tests/test_spark_dedup.py` covers both levels and re-runs. It is skipped automatically when
PySpark or Java isn't installed.
