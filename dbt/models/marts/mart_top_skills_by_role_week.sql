-- For each ISO week (by first_seen_date) and role: how many new postings
-- mention each skill, and what share of that role's postings that is.
with postings as (
    select posting_id, role, date_trunc('week', first_seen_date)::date as week_start
    from {{ ref('fct_postings') }}
),

role_totals as (
    select week_start, role, count(*) as role_postings
    from postings
    group by all
),

skill_counts as (
    select p.week_start, p.role, d.skill_name, d.category, count(*) as postings_with_skill
    from postings as p
    join {{ ref('bridge_posting_skill') }} as b using (posting_id)
    join {{ ref('dim_skill') }} as d using (skill_id)
    group by all
)

select
    s.week_start,
    s.role,
    s.skill_name,
    s.category,
    s.postings_with_skill,
    t.role_postings,
    round(s.postings_with_skill / t.role_postings, 4) as share_of_role_postings,
    rank() over (
        partition by s.week_start, s.role order by s.postings_with_skill desc
    )                                                 as rank_in_role
from skill_counts as s
join role_totals as t using (week_start, role)
