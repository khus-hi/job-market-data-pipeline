-- Staging: parse and type the raw JSON. One row per posting per snapshot day.
-- No business logic here, only renaming, casting and light cleanup.
with source as (
    select * from {{ source('adzuna', 'adzuna_postings') }}
),

parsed as (
    select
        posting_id,
        snapshot_date,
        ingested_at,
        search_role,
        trim(payload ->> '$.title')                              as title,
        payload ->> '$.description'                              as description,
        nullif(trim(payload ->> '$.company.display_name'), '')   as company_name,
        payload ->> '$.location.display_name'                    as location_name,
        payload ->> '$.location.area[1]'                         as state_name,
        payload ->> '$.category.label'                           as category_label,
        payload ->> '$.contract_time'                            as contract_time,
        payload ->> '$.contract_type'                            as contract_type,
        try_cast(payload ->> '$.salary_min' as double)           as salary_min,
        try_cast(payload ->> '$.salary_max' as double)           as salary_max,
        coalesce(payload ->> '$.salary_is_predicted', '0') = '1' as is_salary_predicted,
        try_cast(payload ->> '$.created' as timestamptz)         as posted_at,
        payload ->> '$.redirect_url'                             as redirect_url
    from source
)

-- The same posting can come back under several role searches or pages on
-- one day. Keep one row per posting per day.
select *
from parsed
qualify row_number() over (
    partition by posting_id, snapshot_date
    order by ingested_at desc, search_role
) = 1
