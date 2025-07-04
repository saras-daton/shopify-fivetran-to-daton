{% if var('SHOPIFYV2')  %}
{{ config(enabled = True) }}
{% else %}
{{ config(enabled = False) }}
{% endif %}

{% if is_incremental() %}
  {% set max_loaded_batchruntime = '_daton_batch_runtime >= (select coalesce(max(_daton_batch_runtime), 0) from ' ~ this ~ ')' %}
{% else %}
  {% set max_loaded_batchruntime = '1=1' %}
{% endif %}

{% set table_relations = dbt_utils.get_relations_by_pattern(
  schema_pattern=var('raw_schema'),
  table_pattern=var('shopify_giftcards_tbl_ptrn', '%shopify%gift_cards'),
  exclude=var('shopify_giftcards_exclude_tbl_ptrn', ''),
  database=var('raw_database')) %}

with union_tables as (
  {{ dbt_utils.union_relations(
    relations = table_relations,
    where = max_loaded_batchruntime
  ) }}
)

select
  b.id,
  b.customer_id,
  b.line_item_id,
  b.order_id,
  b.user_id,
  b.api_client_id,
  b.balance,
--   b.code,
  b.created_at,
  b.currency,
  b.disabled_at,
  b.expires_on,
  b.initial_value,
  b.last_characters,
  b.note,
  b.template_suffix,
  b.updated_at,

  b.{{ daton_user_id() }} as _daton_user_id,
  b.{{ daton_batch_runtime() }} as _daton_batch_runtime,
  b.{{ daton_batch_id() }} as _daton_batch_id,
  current_timestamp() as _last_updated,
  '{{ env_var("DBT_CLOUD_RUN_ID", "manual") }}' as _run_id

from (
  select
    a.id,
    a.customer_id,
    a.line_item_id,
    a.order_id,
    a.user_id,
    a.api_client_id,
    a.balance,
    -- a.code,
    created_at,
    a.currency,
   disabled_at,
    a.expires_on,
    a.initial_value,
    a.last_characters,
    a.note,
    a.template_suffix,
    updated_at,
    {{ daton_user_id() }},
    {{ daton_batch_runtime() }},
    {{ daton_batch_id() }},
    row_number() over (
      partition by a.id order by a.{{ daton_batch_runtime() }} desc
    ) as row_num
  from union_tables a
) b
where row_num = 1
