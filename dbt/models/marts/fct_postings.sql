{#
  Fact table: ONE row per posting, no matter how many days we saw it.

  Incremental, window-based:
    1. cutoff = newest snapshot in the data minus `lookback_days`.
    2. Rebuild set = postings with a snapshot since the cutoff, PLUS postings the fact
       table currently says were last seen since the cutoff.
    3. Recompute each of them from its FULL snapshot history (first_seen stays right)
       and merge by posting_id (delete+insert).
    4. post_hook: delete rows that now have no snapshot at all in the window
       (postings that only existed in a day that was re-loaded without them).

  Why steps 2 and 4: if a day is re-loaded and the API returned a different set of
  postings, a posting that disappeared from that day must lose that day too. A plain
  merge on posting_id only touches postings that are still present, which leaves
  stale rows behind (the relationships test to dim_company caught exactly that).

  Backfilling a day older than the lookback window needs --full-refresh.
#}
{{ config(
    materialized='incremental',
    unique_key='posting_id',
    incremental_strategy='delete+insert',
    on_schema_change='fail',
    post_hook="{{ fct_postings_remove_orphans() }}"
) }}

with snapshots as (
    select * from {{ ref('int_postings__enriched') }}
),

{% if is_incremental() %}
rebuild_ids as (
    select posting_id from snapshots where snapshot_date >= {{ fct_postings_cutoff() }}
    union
    select posting_id from {{ this }} where last_seen_date >= {{ fct_postings_cutoff() }}
),

history as (
    -- full history, but only for postings that need rebuilding
    select s.* from snapshots as s
    join rebuild_ids using (posting_id)
),
{% else %}
history as (
    select * from snapshots
),
{% endif %}

seen as (
    select posting_id, min(snapshot_date) as first_seen_date, max(snapshot_date) as last_seen_date
    from history
    group by posting_id
),

latest as (
    -- The most recent snapshot holds the freshest attributes (salary, title edits).
    select * from history
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
    seen.first_seen_date,
    seen.last_seen_date,
    latest.redirect_url
from latest
join seen using (posting_id)
