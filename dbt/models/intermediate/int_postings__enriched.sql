-- Intermediate: add business logic on top of staging. Still one row per
-- posting per snapshot day.
select
    posting_id,
    snapshot_date,
    ingested_at,
    title,
    {{ classify_role('title') }}                         as role,
    company_name,
    md5(lower(coalesce(company_name, 'unknown')))        as company_id,
    location_name,
    state_name,
    contract_time,
    contract_type,
    salary_min,
    salary_max,
    -- Midpoint when both ends exist, otherwise whichever end we have.
    case
        when salary_min is not null and salary_max is not null then (salary_min + salary_max) / 2
        else coalesce(salary_min, salary_max)
    end                                                  as salary_mid,
    is_salary_predicted,
    posted_at,
    redirect_url
from {{ ref('stg_adzuna__postings') }}
