# Running the pipeline in Airflow (Astro CLI)

The repository root is an [Astro](https://www.astronomer.io/docs/astro/cli/overview) project.
`astro dev start` runs Airflow 3 locally in Docker: a scheduler, DAG processor, triggerer, API
server/UI and a Postgres metadata database. It's free and runs entirely on your machine.

## Prerequisites

- Docker Desktop, running
- Astro CLI (`brew install astro` on macOS)
- `.env` set up as in the main README. Astro passes `.env` into every container as environment
  variables, so the DAG gets the Adzuna and AWS keys without them being baked into the image.

## Start

```
astro dev start
```

The first build takes a few minutes. When it's done, open <http://localhost:8080>. With the
current Astro CLI, local dev needs no login.

Inside the containers the DuckDB warehouse lives at `include/data/warehouse.duckdb`. `include/` is
mounted from the repo, so the file survives restarts and you can query it from your Mac.

## The DAG: `job_market_daily`

```
extract → load → dbt_build → source_freshness → sla_check
```

| Feature | How |
|---|---|
| Schedule | `0 13 * * *` (13:00 UTC daily). `catchup=False`, because Adzuna can't return past days. |
| Retries | 2 per task with exponential backoff (2 min, then about 4 min). `extract` gets 3, because the API is the flakiest part. `dbt_build` and freshness get 0: a failing data test is a data problem, and retrying won't fix it. |
| Failure alerts | `on_failure_callback` logs a clear error once a task has used up its retries. In production this would post to Slack or PagerDuty. |
| SLA | 30 minutes. `sla_check` records `met` or `missed` in `meta.pipeline_runs`, and `dagrun_timeout` (1 h) fails a stuck run. Airflow 3 removed the old `sla=` argument, and its replacement, Deadline Alerts, is still experimental in 3.1. |
| One writer | `max_active_runs=1`. DuckDB allows a single writer, so runs, including backfills, go one at a time. |

## Backfills

Adzuna only returns postings that are live *now*, so a backfill can't fetch history. What it can
do is **re-process days whose raw files were already landed**, for example after fixing a dbt
model. For each date the `extract` task decides:

| Date | Raw files for that day | What extract does |
|---|---|---|
| today | any | fetch from the API (replaces an earlier run today) |
| past | exist | skip the API and reuse them |
| past | missing | fail immediately, without retries, with a clear message |

Run a backfill over a date range:

```
astro dev run backfill create --dag-id job_market_daily \
    --from-date 2026-11-07 --to-date 2026-11-11 --reprocess-behavior completed
```

That processes Nov 7 to Nov 10. Two details matter:
- Runs are scheduled at 13:00, and `--to-date` means midnight, so the end date is the day
  *after* the last day you want.
- `--reprocess-behavior completed` re-runs days that already succeeded. The default, `none`,
  skips them, which is wrong when the point of the backfill is to rebuild after a fix.

Add `--dry-run` first to see which dates it will run.

Or use the UI: open the DAG, then **Trigger → Backfill**.

## Test the DAG without the scheduler

```
astro dev run dags test job_market_daily 2026-11-07
```

## Stop

```
astro dev stop        # keeps the Airflow metadata
astro dev kill        # also deletes the metadata database
```

Nothing here runs in the cloud or costs money. The DAG only uses the S3 bucket that's already
set up.
