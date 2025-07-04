{% if var('SHOPIFYV2') %}
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
      "client_details", "current_subtotal_price_set", "current_total_discounts_set",
      "current_total_price_set", "current_total_tax_set", "note_attributes",
      "subtotal_price_set", "tax_lines", "total_discounts_set", "total_line_items_price_set",
      "total_price_set", "total_shipping_price_set", "total_tax_set",
      "billing_address", "shipping_address", "customer", "discount_codes",
      "discount_applications", "fulfillments", "payment_details", "refunds",
      "shipping_lines", "payment_terms"
    ],
    where = max_loaded_batchruntime
  ) }}
)

select 
  line_item.id as id,
  a.id as order_id,
  line_item.product_id,
  line_item.variant_id,
  line_item.fulfillable_quantity,
  line_item.fulfillment_status,
  line_item.gift_card,
  line_item.grams,
  line_item.sku,
  line_item.name,
  line_item.price,
  line_item.price_set,
  line_item.pre_tax_price,
  line_item.pre_tax_price_set,
  line_item.product_exists,
  line_item.quantity,
  line_item.requires_shipping,
  line_item.tax_code,
  line_item.taxable,
  line_item.title,
  line_item.total_discount,
  line_item.total_discount_set,
  line_item.variant_inventory_management,
  line_item.variant_title,
  line_item.vendor,

  -- Line item index
  line_item_offset + 1 as index,

  -- Unnested properties
  line_item_property.name as property_name,
  line_item_property.value as property_value,

  -- Metadata
  a.{{ daton_user_id() }} as _daton_user_id,
  a.{{ daton_batch_runtime() }} as _daton_batch_runtime,
  a.{{ daton_batch_id() }} as _daton_batch_id,
  current_timestamp() as _last_updated,
  '{{ env_var("DBT_CLOUD_RUN_ID", "manual") }}' as _run_id

from union_tables a
left join unnest(a.line_items) as line_item with offset as line_item_offset
left join unnest(line_item.properties) as line_item_property

qualify dense_rank() over (
  partition by coalesce(line_item.id, 0)
  order by a.{{ daton_batch_runtime() }} desc
) = 1
