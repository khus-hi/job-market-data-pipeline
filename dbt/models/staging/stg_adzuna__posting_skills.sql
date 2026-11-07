select distinct
    posting_id,
    snapshot_date,
    skill_name
from {{ source('adzuna', 'posting_skills') }}
