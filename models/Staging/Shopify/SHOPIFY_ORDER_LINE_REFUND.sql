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
  table_pattern=var('shopify_refunds_tbl_ptrn', '%shopify%refunds'),
  exclude=var('shopify_refunds_exclude_tbl_ptrn', ''),
  database=var('raw_database')
) %}

with union_tables as (
  {{ dbt_utils.union_relations(
    relations = table_relations,
    exclude = ["transactions","total_duties_set","order_adjustments","admin_graphql_api_id",
    "line_item","return","refund_shipping_lines"],
    where = max_loaded_batchruntime
  ) }}
)

select 
  coalesce(refund_line_items.id, 0) as id,
  refund_line_items.location_id,
  refund_line_items.line_item_id as order_line_id,
  coalesce(a.id, 0) as refund_id,
  refund_line_items.quantity,
  refund_line_items.restock_type,
  refund_line_items.subtotal,
  refund_line_items.total_tax,
  shop_money.amount as subtotal_shop_money_amount,
  shop_money.currency_code as subtotal_shop_money_currency_code,
  presentment_money.amount as subtotal_presentment_money_amount,
  presentment_money.currency_code as subtotal_presentment_money_currency_code,
  total_tax_shop_money.amount as total_tax_shop_money_amount,
  total_tax_shop_money.currency_code as total_tax_shop_money_currency_code,
  total_tax_presentment_money.amount as total_tax_presentment_money_amount,
  total_tax_presentment_money.currency_code as total_tax_presentment_money_currency_code,
    {{ daton_user_id() }} as _daton_user_id,
  {{ daton_batch_runtime() }} as _daton_batch_runtime,
  {{ daton_batch_id() }} as _daton_batch_id,
  current_timestamp() as _last_updated,
  '{{ env_var("DBT_CLOUD_RUN_ID", "manual") }}' as _run_id


from union_tables a,
  unnest(a.refund_line_items) as refund_line_items,
  unnest(refund_line_items.subtotal_set) as subtotal_set,
  unnest(subtotal_set.shop_money) as shop_money,
  unnest(subtotal_set.presentment_money) as presentment_money,
  unnest(refund_line_items.total_tax_set) as total_tax_set,
  unnest(total_tax_set.shop_money) as total_tax_shop_money,
  unnest(total_tax_set.presentment_money) as total_tax_presentment_money

where refund_line_items.id is not null
