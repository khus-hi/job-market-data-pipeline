-- Many-to-many link between postings and skills. Skills are taken from each
-- posting's latest snapshot, matching the attributes in fct_postings.
select distinct
    f.posting_id,
    d.skill_id
from {{ ref('fct_postings') }} as f
join {{ ref('stg_adzuna__posting_skills') }} as s
    on s.posting_id = f.posting_id
   and s.snapshot_date = f.last_seen_date
join {{ ref('dim_skill') }} as d
    on d.skill_name = s.skill_name
