{% if var('ShopifyOrderTags', true) %}
  {{ config(enabled = true) }}
{% else %}
  {{ config(enabled = false) }}
{% endif %}

{% if is_incremental() %}
  {% set max_loaded_batchruntime = '_daton_batch_runtime >= (select coalesce(max(_daton_batch_runtime), 0) from ' ~ this ~ ')' %}
{% else %}
  {% set max_loaded_batchruntime = '1=1' %}
{% endif %}

{% set table_relations = dbt_utils.get_relations_by_pattern(
  schema_pattern=var('raw_schema'),
  table_pattern=var('shopify_orders_tbl_ptrn', '%shopify%orders'),
  database=var('raw_database')
) %}

with union_tables as (
  {{ dbt_utils.union_relations(
    relations=table_relations,
    exclude=[
      "client_details", "current_subtotal_price_set", "current_total_discounts_set",
      "current_total_price_set", "current_total_tax_set", "note_attributes",
      "subtotal_price_set", "tax_lines", "total_discounts_set", "total_line_items_price_set",
      "total_price_set", "total_shipping_price_set", "total_tax_set", "billing_address",
      "customer", "discount_applications", "fulfillments", "line_items",
      "payment_details", "refunds", "shipping_address", "shipping_lines",
      "payment_terms", "discount_codes"
    ],
    where=max_loaded_batchruntime
  ) }}
),

unnested_tags as (
  select
    coalesce(a.id, 0) as order_id,
    trim(tag) as value,
    offset + 1 as index,
    {{ daton_user_id() }} as _daton_user_id,
    {{ daton_batch_runtime() }} as _daton_batch_runtime,
    {{ daton_batch_id() }} as _daton_batch_id
  from union_tables a,
  unnest(split(a.tags, ',')) as tag with offset
  where a.tags is not null and a.tags != ''
)

select
  order_id,
  index,
  value,
  _daton_user_id,
  _daton_batch_runtime,
  _daton_batch_id,
  current_timestamp() as _last_updated,
  '{{ env_var("DBT_CLOUD_RUN_ID", "manual") }}' as _run_id
from unnested_tags
qualify row_number() over (
  partition by order_id, value
  order by _daton_batch_runtime desc
) = 1
