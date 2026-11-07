{#- Custom generic test: the given columns together identify each row (the table's grain).
    Fails with one row per duplicated combination. -#}
{% test unique_combination(model, columns) %}
select {{ columns | join(', ') }}, count(*) as n
from {{ model }}
group by {{ columns | join(', ') }}
having count(*) > 1
{% endtest %}
