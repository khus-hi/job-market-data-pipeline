{#- Start of the incremental window for fct_postings: newest snapshot minus lookback_days.
    Computed from the source data (not from the fact table) so the pre_hook delete and the
    rebuild query always agree on the same date. -#}
{% macro fct_postings_cutoff() -%}
(select max(snapshot_date) - interval '{{ var("lookback_days") }} days' from {{ ref('int_postings__enriched') }})
{%- endmacro %}

{#- post_hook for fct_postings: drop rows last seen inside the window that no longer have
    any snapshot there (they only existed in a day that was re-loaded without them). -#}
{% macro fct_postings_remove_orphans() -%}
delete from {{ this }}
where last_seen_date >= {{ fct_postings_cutoff() }}
  and posting_id not in (
      select posting_id from {{ ref('int_postings__enriched') }}
      where snapshot_date >= {{ fct_postings_cutoff() }}
  )
{%- endmacro %}
