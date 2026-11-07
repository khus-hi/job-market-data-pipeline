-- Singular test: a skill can't appear in more postings than the role has.
-- Any row returned is a failure.
select *
from {{ ref('mart_top_skills_by_role_week') }}
where share_of_role_postings < 0
   or share_of_role_postings > 1
   or postings_with_skill > role_postings
