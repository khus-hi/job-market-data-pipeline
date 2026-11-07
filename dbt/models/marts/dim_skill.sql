-- One row per skill we track. Comes from the same CSV the Python extractor reads.
select
    md5(skill_name) as skill_id,
    skill_name,
    category
from {{ ref('skill_catalog') }}
