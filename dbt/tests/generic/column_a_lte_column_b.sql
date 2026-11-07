{#- Custom generic test: column_a <= column_b wherever both are present.
    e.g. salary_min <= salary_max, first_seen_date <= last_seen_date. -#}
{% test column_a_lte_column_b(model, column_a, column_b) %}
select *
from {{ model }}
where {{ column_a }} is not null
  and {{ column_b }} is not null
  and {{ column_a }} > {{ column_b }}
{% endtest %}
