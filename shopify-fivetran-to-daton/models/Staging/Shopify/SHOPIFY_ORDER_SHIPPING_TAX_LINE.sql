{% if var('SHOPIFYV2') and var('ShopifyOrders', True) %}
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
  table_pattern=var('shopify_orders_tbl_ptrn', '%shopify%orders'),
  exclude=var('shopify_orders_exclude_tbl_ptrn', '%shopify%t_orders'),
  database=var('raw_database')
) %}

with union_tables as (
  {{ dbt_utils.union_relations(
    relations = table_relations,
    exclude = [
      "client_details", "current_subtotal_price_set", "note_attributes", 
      "current_total_discounts_set", "current_total_price_set", "current_total_tax_set", 
      "subtotal_price_set", "tax_lines", "total_discounts_set", "total_line_items_price_set", "total_price_set",
      "total_shipping_price_set", "total_tax_set", "customer", "discount_applications", "billing_address",
      "shipping_address", "fulfillments", "line_items", "payment_details", "refunds", "discount_codes", "payment_terms",
      "shipping_lines.tax_lines", "shipping_lines.discount_allocations", "shipping_lines.discounted_price_set", "shipping_lines.price_set"
    ],
    where = max_loaded_batchruntime
  ) }}
),

unnested_data as (
  select 
    a.id as order_id,
    shipping_line.id as shipping_line_id,
    tax_line.price,
    tax_line.rate,
    tax_line.title,
    tax_line.channel_liable,
    tax_line_offset + 1 as index,
    a._daton_user_id,
    a._daton_batch_runtime,
    a._daton_batch_id
  from union_tables a,
  unnest(a.shipping_lines) as shipping_line,
  unnest(shipping_line.tax_lines) as tax_line with offset as tax_line_offset
  where tax_line.price is not null
)

select 
  concat(cast(order_id as string), '_', cast(coalesce(shipping_line_id, 0) as string)) as order_shipping_line_id,
  index,
  price,
  rate,
  title,
  _daton_user_id,
  _daton_batch_runtime,
  _daton_batch_id,
  current_timestamp() as _last_updated,
  '{{ env_var("DBT_CLOUD_RUN_ID", "manual") }}' as _run_id
from unnested_data
qualify row_number() over (
  partition by order_id, shipping_line_id, price, rate, title 
  order by _daton_batch_runtime desc
) = 1
