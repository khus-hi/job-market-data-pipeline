-- One row per company (name-based; Adzuna gives no company ID).
select
    company_id,
    coalesce(any_value(company_name), 'Unknown') as company_name,
    min(snapshot_date)                           as first_seen_date,
    max(snapshot_date)                           as last_seen_date,
    count(distinct posting_id)                   as total_postings
from {{ ref('int_postings__enriched') }}
group by company_id
