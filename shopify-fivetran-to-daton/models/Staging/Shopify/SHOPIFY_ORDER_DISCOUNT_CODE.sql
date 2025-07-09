{% if var('SHOPIFYV2') and var('ShopifyOrders',True) %}
{{ config( enabled = True ) }}
{% else %}
{{ config( enabled = False ) }}
{% endif %}

{% if is_incremental() %}
    {% set max_loaded_batchruntime = '_daton_batch_runtime >= (select coalesce(max(_daton_batch_runtime),0) from ' ~ this ~ ')' %}
{% else %}
    {% set max_loaded_batchruntime = '1=1' %}
{% endif %}

{% set table_relations = dbt_utils.get_relations_by_pattern(
schema_pattern=var('raw_schema'),
table_pattern=var('shopify_orders_tbl_ptrn','%shopify%orders'),
exclude=var('shopify_orders_exclude_tbl_ptrn','%shopify%t_orders'),
database=var('raw_database')) %}

with union_tables as (
{{ dbt_utils.union_relations(
    relations = table_relations,
    exclude = ["client_details", "current_subtotal_price_set", "note_attributes", 
"current_total_discounts_set", "current_total_price_set", "current_total_tax_set", 
"subtotal_price_set", "tax_lines", "total_discounts_set", "total_line_items_price_set", "total_price_set",
"total_shipping_price_set", "total_tax_set", "customer", "discount_applications", "billing_address",
"shipping_address", "fulfillments", "line_items", "payment_details", "refunds",
"shipping_lines", "payment_terms"],
    where = max_loaded_batchruntime) }}
),

base_data as (
select 
  cast(id as numeric) as order_id,
  discount_codes,
  _daton_user_id,
  _daton_batch_runtime,
  _daton_batch_id
from union_tables
where discount_codes is not null 
  and array_length(discount_codes) > 0
),

unnested_data as (
select 
  order_id,
  offset + 1 as index,
  discount_code.code,
  discount_code.amount,
  discount_code.type,
  _daton_user_id,
  _daton_batch_runtime,
  _daton_batch_id
from base_data,
unnest(discount_codes) as discount_code with offset as offset
where discount_code.code is not null
)

select 
index,
order_id,
code,
amount,
type,
_daton_user_id,
_daton_batch_runtime,
_daton_batch_id,
current_timestamp() as _last_updated,
'{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id
from unnested_data
qualify row_number() over (
  partition by order_id, code, amount, type
  order by _daton_batch_runtime desc
) = 1