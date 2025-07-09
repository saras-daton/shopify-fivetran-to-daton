{% if var('SHOPIFYV2') and var('ShopifyRefunds', True) %}
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
  table_pattern=var('shopify_refunds_tbl_ptrn', '%shopify%refunds'),
  exclude=var('shopify_refunds_exclude_tbl_ptrn', ''),
  database=var('raw_database')
) %}

with union_tables as (
  {{ dbt_utils.union_relations(
    relations = table_relations,
    exclude = ["refund_line_items", "transactions", "order_adjustments", "return", "refund_shipping_lines"],
    where = max_loaded_batchruntime
  ) }}
)

select
  coalesce(a.id, 0) as id,
  coalesce(a.order_id, 0) as order_id,
  coalesce(a.user_id, 0) as user_id,
  {{ timezone_conversion("a.created_at") }} as created_at,
  {{ timezone_conversion("a.processed_at") }} as processed_at,
  a.note,
  a.restock,

  -- total_duties_set.shop_money
  shop_money.amount as total_duties_shop_money_amount,
  shop_money.currency_code as total_duties_shop_money_currency_code,

  -- total_duties_set.presentment_money
  presentment_money.amount as total_duties_presentment_money_amount,
  presentment_money.currency_code as total_duties_presentment_money_currency_code,

  -- Metadata
  a.{{ daton_user_id() }} as _daton_user_id,
  a.{{ daton_batch_runtime() }} as _daton_batch_runtime,
  a.{{ daton_batch_id() }} as _daton_batch_id,
  current_timestamp() as _last_updated,
  '{{ env_var("DBT_CLOUD_RUN_ID", "manual") }}' as _run_id

from union_tables a,
  unnest(a.total_duties_set) as total_duties_set,
  unnest(total_duties_set.shop_money) as shop_money,
  unnest(total_duties_set.presentment_money) as presentment_money

qualify row_number() over (
  partition by coalesce(a.id, 0)
  order by a.{{ daton_batch_runtime() }} desc
) = 1
