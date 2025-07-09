{% if var('SHOPIFYV2') %}
  {{ config(enabled = true) }}
{% else %}
  {{ config(enabled = false) }}
{% endif %}

{% if is_incremental() %}
  {% set max_loaded_batchruntime = '_daton_batch_runtime >= (select coalesce(max(_daton_batch_runtime),0) from ' ~ this ~ ')' %}
{% else %}
  {% set max_loaded_batchruntime = '1=1' %}
{% endif %}

{% set table_relations = dbt_utils.get_relations_by_pattern(
  schema_pattern=var('raw_schema'),
  table_pattern=var('shopify_fulfillment_order_line_items_tbl_ptrn', '%shopify%fulfillment_order%'),
  exclude='',
  database=var('raw_database')
) %}

with union_tables as (
  {{ dbt_utils.union_relations(
    relations = table_relations,
    where = max_loaded_batchruntime
  ) }}
)

select
  -- Required fields from nested line_items
  li.id as id,
  li.fulfillment_order_id,
  li.inventory_item_id,
  li.fulfillable_quantity as remaining_quantity,
  li.quantity as total_quantity,

  -- Metadata
  a.{{ daton_user_id() }} as _daton_user_id,
  a.{{ daton_batch_runtime() }} as _daton_batch_runtime,
  a.{{ daton_batch_id() }} as _daton_batch_id,
  current_timestamp() as _last_updated,
  '{{ env_var("DBT_CLOUD_RUN_ID", "manual") }}' as _run_id

from union_tables a
left join unnest(a.line_items) as li
qualify row_number() over (partition by coalesce(li.id, 0) order by a.{{ daton_batch_runtime() }} desc) = 1