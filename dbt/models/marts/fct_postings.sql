{#
  Fact table: ONE row per posting, no matter how many days we saw it.

  Incremental: each run only reprocesses the last `lookback_days` of snapshots
  and merges them in by posting_id (delete+insert). Re-running a day replaces
  the same rows instead of adding duplicates. first_seen_date is preserved from
  the existing table so it never moves forward on a re-run.

  If you backfill a day older than the lookback window, run with --full-refresh.
#}
{{ config(
    materialized='incremental',
    unique_key='posting_id',
    incremental_strategy='delete+insert',
    on_schema_change='fail'
) }}

with batch as (
    select * from {{ ref('int_postings__enriched') }}
    {% if is_incremental() %}
    where snapshot_date >= (
        select max(last_seen_date) - interval '{{ var("lookback_days") }} days' from {{ this }}
    )
    {% endif %}
),

seen as (
    select posting_id, min(snapshot_date) as first_seen_date, max(snapshot_date) as last_seen_date
    from batch
    group by posting_id
),

latest as (
    -- The most recent snapshot holds the freshest attributes (salary, title edits).
    select * from batch
    qualify row_number() over (partition by posting_id order by snapshot_date desc) = 1
)

select
    latest.posting_id,
    latest.title,
    latest.role,
    latest.company_id,
    latest.location_name,
    latest.state_name,
    latest.contract_time,
    latest.contract_type,
    latest.salary_min,
    latest.salary_max,
    latest.salary_mid,
    latest.is_salary_predicted,
    latest.posted_at,
    {% if is_incremental() %}
    least(seen.first_seen_date, coalesce(existing.first_seen_date, seen.first_seen_date))
    {% else %}
    seen.first_seen_date
    {% endif %}                                              as first_seen_date,
    {% if is_incremental() %}
    greatest(seen.last_seen_date, coalesce(existing.last_seen_date, seen.last_seen_date))
    {% else %}
    seen.last_seen_date
    {% endif %}                                              as last_seen_date,
    latest.redirect_url
from latest
join seen using (posting_id)
{% if is_incremental() %}
left join {{ this }} as existing using (posting_id)
{% endif %}
