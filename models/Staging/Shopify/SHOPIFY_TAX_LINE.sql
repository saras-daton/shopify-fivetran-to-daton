{% if var('SHOPIFYV2', True) %}
  {{ config(enabled = True) }}
{% else %}
  {{ config(enabled = False) }}
{% endif %}

{% if is_incremental() %}
  {% set max_loaded_batchruntime = '_daton_batch_runtime >= (SELECT COALESCE(MAX(_daton_batch_runtime), 0) FROM ' ~ this ~ ')' %}
{% else %}
  {% set max_loaded_batchruntime = '1=1' %}
{% endif %}

{% set table_relations = dbt_utils.get_relations_by_pattern(
  schema_pattern = var('raw_schema'),
  table_pattern = var('shopify_orders_tbl_ptrn', '%shopify%orders'),
  exclude = var('shopify_orders_exclude_tbl_ptrn', '%shopify%t_orders'),
  database = var('raw_database')
) %}

WITH union_tables AS (
  {{ dbt_utils.union_relations(
    relations = table_relations,
    exclude = [
      "client_details", "current_subtotal_price_set", "current_total_discounts_set",
      "current_total_price_set", "current_total_tax_set", "note_attributes",
      "subtotal_price_set", "tax_lines", "total_discounts_set",
      "total_line_items_price_set", "total_price_set", "total_shipping_price_set",
      "total_tax_set", "billing_address", "customer", "discount_applications",
      "fulfillments", "payment_details", "refunds", "shipping_address", "shipping_lines",
      "discount_codes", "payment_terms"
    ],
    where = max_loaded_batchruntime
  ) }}
)

SELECT
  line_item.id AS order_line_id,
  ROW_NUMBER() OVER (
    PARTITION BY line_item.id 
    ORDER BY tax_line.title
  ) AS index,
  tax_line.price,
  tax_line.rate,
  tax_line.title,

  order_table.{{ daton_user_id() }} AS _daton_user_id,
  order_table.{{ daton_batch_runtime() }} AS _daton_batch_runtime,
  order_table.{{ daton_batch_id() }} AS _daton_batch_id,
  CURRENT_TIMESTAMP() AS _last_updated,
  '{{ env_var("DBT_CLOUD_RUN_ID", "manual") }}' AS _run_id

FROM union_tables AS order_table
LEFT JOIN UNNEST(order_table.line_items) AS line_item
LEFT JOIN UNNEST(line_item.tax_lines) AS tax_line

WHERE tax_line.title IS NOT NULL

QUALIFY ROW_NUMBER() OVER (
  PARTITION BY line_item.id, tax_line.title
  ORDER BY order_table.{{ daton_batch_runtime() }} DESC
) = 1
