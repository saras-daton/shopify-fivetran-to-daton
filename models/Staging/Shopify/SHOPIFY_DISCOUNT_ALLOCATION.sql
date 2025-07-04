{% if var('SHOPIFYV2') %}
  {{ config(enabled = true, materialized='incremental') }}
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
      "total_price_set", "total_shipping_price_set", "total_tax_set", "billing_address",
      "shipping_address", "customer", "discount_codes", "fulfillments", "payment_details",
      "refunds", "shipping_lines", "payment_terms", "discount_applications"
    ],
    where = max_loaded_batchruntime
  ) }}
),

flattened as (
  select 
    coalesce(line_items.id, 0) as order_line_id,
     offset_number + 1 as index,
    discount_allocations.discount_application_index,
    discount_allocations.amount,
    shop_money.amount as amount_set_shop_money_amount,
    shop_money.currency_code as amount_set_shop_money_currency_code,
    presentment_money.amount as amount_set_presentment_money_amount,
    presentment_money.currency_code as amount_set_presentment_money_currency_code,
    a.{{ daton_user_id() }} as _daton_user_id,
    a.{{ daton_batch_runtime() }} as _daton_batch_runtime,
    a.{{ daton_batch_id() }} as _daton_batch_id,
    current_timestamp() as _last_updated,
    '{{ env_var("DBT_CLOUD_RUN_ID", "manual") }}' as _run_id
   

  from union_tables a
  {{ unnesting("line_items") }}
{{ multi_unnesting("line_items", "discount_allocations") }} WITH OFFSET AS offset_number
  {{ multi_unnesting("discount_allocations", "amount_set") }}
  {{ multi_unnesting("amount_set", "shop_money") }}
  {{ multi_unnesting("amount_set", "presentment_money") }}
)

select *
from flattened
qualify dense_rank() over (
  partition by order_line_id, discount_application_index
  order by _daton_batch_runtime desc
) = 1
