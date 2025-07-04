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
  table_pattern=var('shopify_fulfillment_events_tbl_ptrn', '%shopify%fulfillment_events'),
  exclude=var('shopify_fulfillment_events_exclude_tbl_ptrn', ''),
  database=var('raw_database')) %}

with union_tables as (
  {{ dbt_utils.union_relations(
    relations = table_relations,
    where = max_loaded_batchruntime
  ) }}
)

select
  b.id,
  b.fulfillment_id,
  b.order_id,
  b.shop_id,
--   b.address1,
  b.city,
  b.country,
  b.created_at,
  b.estimated_delivery_at,
  b.happened_at,
  b.latitude,
  b.longitude,
  b.message,
  b.province,
  b.status,
  b.updated_at,
  b.zip,

  b.{{ daton_user_id() }} as _daton_user_id,
  b.{{ daton_batch_runtime() }} as _daton_batch_runtime,
  b.{{ daton_batch_id() }} as _daton_batch_id,
  current_timestamp() as _last_updated,
  '{{ env_var("DBT_CLOUD_RUN_ID", "manual") }}' as _run_id

from (
  select
    a.id as id,
    a.fulfillment_id,
    a.order_id,
    a.shop_id,
    a.city,
    a.country,
    created_at,
     estimated_delivery_at,
    happened_at,
    a.latitude,
    a.longitude,
    a.message,
    a.province,
    a.status,
    updated_at,
    a.zip,
    {{ daton_user_id() }},
    {{ daton_batch_runtime() }},
    {{ daton_batch_id() }},
    row_number() over (
      partition by a.id, a.fulfillment_id
      order by a.{{ daton_batch_runtime() }} desc
    ) as row_num
  from union_tables a
) b
where row_num = 1
