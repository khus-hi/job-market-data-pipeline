-- "Top skills this week": share of all new postings in the most recent week that
-- mention each skill. Plain SQL over the marts, so any DuckDB client can run it.
-- Uses the latest week in the data (not today's date) so it works on any history.
with latest_week as (
    select max(date_trunc('week', first_seen_date))::date as week_start
    from marts.fct_postings
),

week_postings as (
    select f.posting_id
    from marts.fct_postings as f, latest_week as w
    where date_trunc('week', f.first_seen_date) = w.week_start
)

select
    (select week_start from latest_week)                         as week_start,
    d.skill_name,
    count(*)                                                     as postings,
    count(*) / (select count(*) from week_postings)              as share_of_postings
from week_postings as p
join marts.bridge_posting_skill as b using (posting_id)
join marts.dim_skill as d using (skill_id)
group by d.skill_name
order by postings desc, d.skill_name
