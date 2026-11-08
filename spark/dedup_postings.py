"""PySpark job: deduplicate job postings across every day in the raw landing zone.

It reads the raw JSON straight from the landing zone (not from the warehouse) and
removes two kinds of duplicates:

1. **Same posting, many sightings.** Adzuna returns a live posting every day it stays
   open, and often under several role searches on the same day. Collapse all of those
   into one row per posting id with first_seen_date, last_seen_date, days_seen and the
   attributes from the most recent sighting.

2. **Reposts.** Employers often close a posting and open a new one for the same job, which
   gets a new Adzuna id. Postings with the same company, normalised title and location
   form one *repost group* as long as each one appears within `--repost-gap-days` of the
   previous one ending. Every posting gets a `canonical_posting_id` (the first posting in
   its group), so "how many distinct jobs" can be counted, not just "how many ids".

Output: Parquet at data/spark/postings_dedup/, overwritten on every run (idempotent).
A row count is logged to meta.pipeline_runs, like every other step.

    python -m spark.dedup_postings                    # whatever STORAGE_BACKEND says
    python -m spark.dedup_postings --check-dbt        # also compare with marts.fct_postings

Step 1 is the same logic as dbt's fct_postings, written in Spark. `--check-dbt` proves the
two agree. When to pick which is in docs/spark.md.
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
import uuid
from datetime import UTC, date, datetime
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

log = logging.getLogger("spark_dedup")

# Read only the fields we need, with explicit types. Without a schema Spark would scan
# every file once just to infer one, and a field that is sometimes a number and sometimes
# a string would change type from run to run.
POSTING_SCHEMA = T.StructType(
    [
        T.StructField("id", T.StringType()),
        T.StructField("title", T.StringType()),
        T.StructField("created", T.StringType()),
        T.StructField("redirect_url", T.StringType()),
        T.StructField("company", T.StructType([T.StructField("display_name", T.StringType())])),
        T.StructField(
            "location",
            T.StructType(
                [
                    T.StructField("display_name", T.StringType()),
                    T.StructField("area", T.ArrayType(T.StringType())),
                ]
            ),
        ),
        T.StructField("contract_time", T.StringType()),
        T.StructField("contract_type", T.StringType()),
        T.StructField("salary_min", T.DoubleType()),
        T.StructField("salary_max", T.DoubleType()),
        T.StructField("salary_is_predicted", T.StringType()),
    ]
)
ENVELOPE_SCHEMA = T.StructType(
    [
        T.StructField("snapshot_date", T.StringType()),
        T.StructField("ingested_at", T.StringType()),
        T.StructField("search_role", T.StringType()),
        T.StructField("page", T.IntegerType()),
        T.StructField(
            "response",
            T.StructType([T.StructField("results", T.ArrayType(POSTING_SCHEMA))]),
        ),
    ]
)


def build_spark(app_name: str = "job-market-dedup") -> SparkSession:
    """A local Spark session sized for a laptop."""
    return (
        SparkSession.builder.master("local[*]")
        .appName(app_name)
        # The default is 200 shuffle partitions, built for clusters. For a few thousand
        # rows that's 200 tiny tasks per shuffle, so keep it small.
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )


def read_sightings(spark: SparkSession, partition_dirs: list[str]) -> DataFrame:
    """Raw JSON files -> one row per posting per search page per day (the raw grain)."""
    envelopes = spark.read.schema(ENVELOPE_SCHEMA).option("multiLine", True).json(partition_dirs)
    return (
        envelopes.select(
            F.to_date("snapshot_date").alias("snapshot_date"),
            F.try_to_timestamp("ingested_at").alias("ingested_at"),
            "search_role",
            F.explode("response.results").alias("p"),
        )
        .where(F.col("p.id").isNotNull())
        .select(
            F.col("p.id").alias("posting_id"),
            "snapshot_date",
            "ingested_at",
            "search_role",
            F.trim("p.title").alias("title"),
            F.nullif(F.trim("p.company.display_name"), F.lit("")).alias("company_name"),
            F.col("p.location.display_name").alias("location_name"),
            # get() returns NULL when the index is missing. Plain [1] fails the whole job,
            # because Spark 4 runs in ANSI mode, and some postings only have area=["US"].
            F.get("p.location.area", 1).alias("state_name"),
            F.col("p.contract_time").alias("contract_time"),
            F.col("p.contract_type").alias("contract_type"),
            F.col("p.salary_min").alias("salary_min"),
            F.col("p.salary_max").alias("salary_max"),
            (F.coalesce(F.col("p.salary_is_predicted"), F.lit("0")) == "1").alias(
                "is_salary_predicted"
            ),
            F.try_to_timestamp("p.created").alias("posted_at"),
            F.col("p.redirect_url").alias("redirect_url"),
        )
    )


def dedup_postings(sightings: DataFrame) -> DataFrame:
    """Level 1: one row per posting id, with the attributes of its latest sighting."""
    # Newest day first; within a day, the latest fetch, then the role search name, so the
    # pick is deterministic (the same tie-break as dbt's stg_adzuna__postings).
    newest_first = Window.partitionBy("posting_id").orderBy(
        F.col("snapshot_date").desc(), F.col("ingested_at").desc(), F.col("search_role")
    )
    latest = (
        sightings.withColumn("rn", F.row_number().over(newest_first))
        .where("rn = 1")
        .drop("rn", "snapshot_date", "ingested_at", "search_role")
    )
    seen = sightings.groupBy("posting_id").agg(
        F.min("snapshot_date").alias("first_seen_date"),
        F.max("snapshot_date").alias("last_seen_date"),
        F.countDistinct("snapshot_date").alias("days_seen"),
        F.count(F.lit(1)).alias("times_returned"),  # every search page it came back in
    )
    return latest.join(seen, "posting_id")


def normalise_title(col: F.Column) -> F.Column:
    """'Sr. Data Engineer (Remote)' -> 'sr data engineer remote'."""
    return F.trim(F.regexp_replace(F.lower(col), r"[^a-z0-9]+", " "))


def mark_reposts(postings: DataFrame, gap_days: int) -> DataFrame:
    """Level 2: group postings that look like the same job re-listed under a new id.

    This is "sessionisation": within one (company, title, location) key, sorted by
    first_seen_date, a posting starts a new group when it first appears more than
    `gap_days` after every earlier posting in the key was last seen.
    """
    key = [
        F.lower(F.col("company_name")),
        normalise_title(F.col("title")),
        F.lower(F.coalesce(F.col("location_name"), F.lit(""))),
    ]
    with_key = postings.withColumn("job_key", F.sha2(F.concat_ws("|", *key), 256))
    # No company name means no reliable way to match: each such posting is its own job.
    with_key = with_key.withColumn(
        "job_key",
        F.when(F.col("company_name").isNull(), F.col("posting_id")).otherwise(F.col("job_key")),
    )

    in_key = Window.partitionBy("job_key").orderBy("first_seen_date", "posting_id")
    before = in_key.rowsBetween(Window.unboundedPreceding, -1)
    grouped = (
        with_key.withColumn("prev_last_seen", F.max("last_seen_date").over(before))
        .withColumn(
            "starts_group",
            F.col("prev_last_seen").isNull()
            | (F.datediff("first_seen_date", "prev_last_seen") > gap_days),
        )
        # A running count of group starts numbers the groups inside each key.
        .withColumn("group_no", F.sum(F.col("starts_group").cast("int")).over(in_key))
    )
    group = Window.partitionBy("job_key", "group_no")
    first_in_group = group.orderBy("first_seen_date", "posting_id")
    return (
        grouped.withColumn("canonical_posting_id", F.first("posting_id").over(first_in_group))
        .withColumn("postings_in_group", F.count(F.lit(1)).over(group))
        .withColumn("is_repost", F.col("posting_id") != F.col("canonical_posting_id"))
        .drop("job_key", "prev_last_seen", "starts_group", "group_no")
    )


def run_job(spark: SparkSession, partition_dirs: list[str], out_dir: Path, gap_days: int) -> dict:
    sightings = read_sightings(spark, partition_dirs).cache()
    result = mark_reposts(dedup_postings(sightings), gap_days)
    # One small file is right for this size. Overwrite makes a re-run replace the output.
    result.coalesce(1).write.mode("overwrite").parquet(str(out_dir))

    written = spark.read.parquet(str(out_dir))
    stats = written.agg(
        F.count(F.lit(1)).alias("postings"),
        F.countDistinct("canonical_posting_id").alias("distinct_jobs"),
        F.sum(F.col("is_repost").cast("int")).alias("reposts"),
    ).first()
    stats = {
        "days": len(partition_dirs),
        "sightings": sightings.count(),
        "postings": stats["postings"],
        "distinct_jobs": stats["distinct_jobs"],
        "reposts": stats["reposts"] or 0,
        # Per-day input size, so --check-dbt can tell whether the warehouse loaded the
        # same version of each day that's in the landing zone now.
        "sightings_per_day": {
            r["snapshot_date"].isoformat(): r["count"]
            for r in sightings.groupBy("snapshot_date").count().collect()
        },
    }
    sightings.unpersist()
    return stats


def local_partitions(s, cache_root: Path) -> list[str]:
    """Folders Spark should read, one per landed day.

    With S3 as the landing zone the days are mirrored into a local cache first. Reading
    s3a:// directly would need the hadoop-aws jars (hundreds of MB), which isn't worth it
    for a few MB of JSON. See docs/spark.md.
    """
    from ingestion.config import get_store
    from ingestion.storage import LocalStore

    store = get_store(s)
    if not isinstance(store, LocalStore):
        cache = LocalStore(cache_root)
        days = store.list_partitions()
        for dt in days:
            cache.write_partition(dt, dict(store.read_partition(dt)))
        log.info("mirrored %d day(s) from the S3 landing zone into %s", len(days), cache_root)
        store = cache
    return [store.describe(dt) for dt in store.list_partitions()]  # skips crashed *.tmp dirs


def day_of(partition_dir: str) -> date:
    return datetime.strptime(Path(partition_dir).name, "dt=%Y-%m-%d").date()


def warehouse_days(db_path: Path) -> dict[str, int]:
    """Raw rows per day in the DuckDB warehouse, which is what dbt's fct_postings is built from."""
    import duckdb

    with duckdb.connect(str(db_path), read_only=True) as con:
        rows = con.execute(
            "select snapshot_date, count(*) from raw.adzuna_postings group by 1"
        ).fetchall()
    return {d.isoformat(): n for d, n in rows}


def days_out_of_sync(landed: dict[str, int], loaded: dict[str, int]) -> list[str]:
    """Days whose raw row count differs between the landing zone and the warehouse: never
    loaded, or re-extracted after they were loaded (e.g. by an Airflow run)."""
    return sorted(d for d, n in landed.items() if loaded.get(d) != n)


def compare_with_dbt(db_path: Path, out_dir: Path) -> int:
    """Rows where Spark and dbt's fct_postings disagree on the posting set or seen dates.

    Only meaningful when both saw the same days: check that with warehouse_days() first.
    """
    import duckdb

    with duckdb.connect(str(db_path), read_only=True) as con:
        return con.execute(
            f"""
            with s as (
                select posting_id, first_seen_date, last_seen_date
                from read_parquet('{out_dir}/*.parquet')
            ),
            d as (select posting_id, first_seen_date, last_seen_date from marts.fct_postings)
            select count(*) from ((select * from s except select * from d)
                                  union all (select * from d except select * from s))
            """
        ).fetchone()[0]


def main(argv: list[str] | None = None) -> int:
    from ingestion.config import get_settings
    from ingestion.quality import log_step

    parser = argparse.ArgumentParser(description="Deduplicate postings across days with Spark.")
    parser.add_argument("--repost-gap-days", type=int, default=14)
    parser.add_argument("--check-dbt", action="store_true", help="compare with fct_postings")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    s = get_settings()
    spark_dir = s.data_dir / "spark"
    out_dir = spark_dir / "postings_dedup"
    run_id = f"spark-{datetime.now(UTC):%Y%m%dT%H%M%S}-{uuid.uuid4().hex[:6]}"

    started = time.monotonic()
    dirs = local_partitions(s, spark_dir / "raw_cache")
    if not dirs:
        log.error("No raw data found. Run the pipeline first.")
        return 1
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    try:
        stats = run_job(spark, dirs, out_dir, args.repost_gap_days)
    finally:
        spark.stop()
    seconds = time.monotonic() - started

    newest = day_of(dirs[-1])
    log_step(s.duckdb_path, run_id, newest, "spark_dedup", "success", stats["postings"],
             seconds, stats)  # fmt: skip
    log.info("wrote %s in %.1fs: %s", out_dir, seconds, stats)
    print(
        f"\n{stats['days']} day(s), {stats['sightings']} raw sightings -> "
        f"{stats['postings']} unique postings -> {stats['distinct_jobs']} distinct jobs "
        f"({stats['reposts']} reposts)"
    )

    if args.check_dbt:
        # Spark reads the landing zone; dbt reads the warehouse. If a day was never loaded,
        # or was re-extracted after loading (Airflow re-runs land into the same bucket but
        # load a different warehouse file), the two differ for a boring reason. Say so
        # instead of reporting a misleading diff.
        loaded = warehouse_days(s.duckdb_path)
        missing = days_out_of_sync(stats["sightings_per_day"], loaded)
        if missing:
            print(f"Can't compare yet: {s.duckdb_path} doesn't match the landing zone for:")
            for dt in missing:
                landed = stats["sightings_per_day"][dt]
                print(f"  {dt}: {landed} rows landed, {loaded.get(dt, 0)} rows loaded")
            print("(Re-)load each day, then re-run:")
            for dt in missing:
                print(f"  python -m ingestion.pipeline --date {dt} --skip-extract")
            return 1
        diff = compare_with_dbt(s.duckdb_path, out_dir)
        print(f"Spark vs dbt fct_postings: {diff} rows differ")
        return 0 if diff == 0 else 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
