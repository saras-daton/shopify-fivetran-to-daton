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
  table_pattern=var('shopify_refunds_tbl_ptrn', '%shopify%refunds'),
  exclude=var('shopify_refunds_exclude_tbl_ptrn', ''),
  database=var('raw_database')
) %}

with union_tables as (
  {{ dbt_utils.union_relations(
    relations = table_relations,
    exclude = ["transactions", "total_duties_set", "refund_line_items"],
    where = max_loaded_batchruntime
  ) }}
)

select 
  coalesce(order_adjustments.id, 0) as id,
  coalesce(a.order_id, 0) as order_id,
  coalesce(a.id, 0) as refund_id,
  order_adjustments.amount,
  order_adjustments.tax_amount,
  order_adjustments.kind,
  order_adjustments.reason,

  -- Flattened amount_set.shop_money
  amount_shop_money.amount as amount_set_shop_money_amount,
  amount_shop_money.currency_code as amount_set_shop_money_currency_code,

  -- Flattened amount_set.presentment_money
  amount_presentment_money.amount as amount_set_presentment_money_amount,
  amount_presentment_money.currency_code as amount_set_presentment_money_currency_code,

  -- Flattened tax_amount_set.shop_money
  tax_shop_money.amount as tax_amount_set_shop_money_amount,
  tax_shop_money.currency_code as tax_amount_set_shop_money_currency_code,

  -- Flattened tax_amount_set.presentment_money
  tax_presentment_money.amount as tax_amount_set_presentment_money_amount,
  tax_presentment_money.currency_code as tax_amount_set_presentment_money_currency_code,

  -- Metadata
  a.{{ daton_user_id() }} as _daton_user_id,
  a.{{ daton_batch_runtime() }} as _daton_batch_runtime,
  a.{{ daton_batch_id() }} as _daton_batch_id,
  current_timestamp() as _last_updated,
  '{{ env_var("DBT_CLOUD_RUN_ID", "manual") }}' as _run_id

from union_tables a,
  unnest(a.order_adjustments) as order_adjustments,
  unnest(order_adjustments.amount_set) as amount_set,
  unnest(amount_set.shop_money) as amount_shop_money,
  unnest(amount_set.presentment_money) as amount_presentment_money,
  unnest(order_adjustments.tax_amount_set) as tax_amount_set,
  unnest(tax_amount_set.shop_money) as tax_shop_money,
  unnest(tax_amount_set.presentment_money) as tax_presentment_money

qualify row_number() over (
  partition by coalesce(order_adjustments.id, 0)
  order by a.{{ daton_batch_runtime() }} desc
) = 1
