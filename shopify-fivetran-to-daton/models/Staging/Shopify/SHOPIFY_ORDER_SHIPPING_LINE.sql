{% if var('SHOPIFYV2') %}
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
  exclude=var('shopify_orders_exclude_tbl_ptrn', '%shopify%t_orders'),
  database=var('raw_database')
) %}

with union_tables as (
  {{ dbt_utils.union_relations(
    relations=table_relations,
    exclude=[
      "client_details", "current_subtotal_price_set", "note_attributes", 
      "current_total_discounts_set", "current_total_price_set", "current_total_tax_set", 
      "subtotal_price_set", "tax_lines", "total_discounts_set", "total_line_items_price_set", 
      "total_price_set", "total_shipping_price_set", "total_tax_set", "customer", 
      "discount_applications", "billing_address", "shipping_address", "fulfillments", 
      "line_items", "payment_details", "refunds", "discount_codes", "payment_terms"
    ],
    where=max_loaded_batchruntime
  ) }}
)

select
  coalesce(a.id, 0) as order_id,
  shipping_line.id as id,
  shipping_line.carrier_identifier,
  shipping_line.code,
  shipping_line.discounted_price,
  discounted_price_shop_money.amount as discounted_price_set__shop_money__amount,
  shipping_line.price,
  price_shop_money.amount as price_set__shop_money__amount,
--   shipping_line.requested_fulfillment_service_id,
  shipping_line.source,
  shipping_line.title,
  {{ daton_user_id() }} as _daton_user_id,
  {{ daton_batch_runtime() }} as _daton_batch_runtime,
  {{ daton_batch_id() }} as _daton_batch_id,
  current_timestamp() as _last_updated,
  '{{ env_var("DBT_CLOUD_RUN_ID", "manual") }}' as _run_id

from union_tables a,
  unnest(a.shipping_lines) as shipping_line
left join unnest(shipping_line.discounted_price_set) as discounted_price_set
left join unnest(discounted_price_set.shop_money) as discounted_price_shop_money
left join unnest(shipping_line.price_set) as price_set
left join unnest(price_set.shop_money) as price_shop_money

qualify dense_rank() over (partition by coalesce(a.id, 0) order by {{ daton_batch_runtime() }} desc) = 1
