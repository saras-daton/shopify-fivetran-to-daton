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
    relations = table_relations,
    exclude = [
      "client_details", "current_subtotal_price_set", "current_total_discounts_set",
      "current_total_price_set", "current_total_tax_set", "note_attributes",
      "subtotal_price_set", "tax_lines", "total_discounts_set", "total_line_items_price_set",
      "total_price_set", "total_shipping_price_set", "total_tax_set", "discount_codes",
      "payment_details", "refunds", "shipping_lines", "payment_terms", "line_items"
    ],
    where = max_loaded_batchruntime
  ) }}
)

select
  order_line_id,
  fulfillment_id,
  order_id,
  product_id,
  variant_id,
  fulfillable_quantity,
  gift_card,
  grams,
  name,
  price_set,
  price,
  properties,
  quantity,
  requires_shipping,
  sku,
  taxable,
  title,
  variant_title,
  vendor,
  _daton_user_id,
  _daton_batch_runtime,
  _daton_batch_id,
  current_timestamp() as _last_updated,
  '{{ env_var("DBT_CLOUD_RUN_ID", "manual") }}' as _run_id

from (
  select
    line_items.id as order_line_id,
    fulfillments.id as fulfillment_id,
    a.id as order_id,

    line_items.product_id,
    line_items.variant_id,
    line_items.fulfillable_quantity,
    line_items.gift_card,
    line_items.grams,
    line_items.name,
    line_items.price_set,
    line_items.price,
    line_items.properties,
    line_items.quantity,
    line_items.requires_shipping,
    line_items.sku,
    line_items.taxable,
    line_items.title,
    line_items.variant_title,
    line_items.vendor,

    a.{{ daton_user_id() }} as _daton_user_id,
    a.{{ daton_batch_runtime() }} as _daton_batch_runtime,
    a.{{ daton_batch_id() }} as _daton_batch_id,

    row_number() over (
      partition by a.id, fulfillments.id, line_items.id
      order by a.{{ daton_batch_runtime() }} desc
    ) as row_num

  from union_tables a
  left join unnest(a.fulfillments) as fulfillments
  left join unnest(fulfillments.line_items) as line_items
) final
where row_num = 1
