{#- Map a free-text job title to one of five role buckets. Order matters:
    "Analytics Engineer" must be checked before the generic "engineer" rules. -#}
{% macro classify_role(title_col) -%}
case
    when regexp_matches(lower({{ title_col }}), 'analytics engineer') then 'analytics_engineer'
    when regexp_matches(lower({{ title_col }}), 'data scien|machine learning|\bml\b') then 'data_scientist'
    when regexp_matches(lower({{ title_col }}), 'data engineer|etl developer|data platform|big data|data pipeline') then 'data_engineer'
    when regexp_matches(lower({{ title_col }}), 'data analyst|business intelligence|\bbi\b|analytics analyst|reporting analyst|business analyst') then 'data_analyst'
    else 'other'
end
{%- endmacro %}
