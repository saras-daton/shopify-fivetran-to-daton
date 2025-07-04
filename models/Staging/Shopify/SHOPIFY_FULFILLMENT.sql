{% if var('SHOPIFYV2')  %}
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
  table_pattern=var('shopify_orders_tbl_ptrn', '%shopify%orders'),
  exclude=var('shopify_orders_exclude_tbl_ptrn', '%shopify%t_orders'),
  database=var('raw_database')
) %}
with union_tables as (
{{ dbt_utils.union_relations(
    relations = table_relations,
    exclude = ["client_details", "current_subtotal_price_set", 
"current_total_discounts_set", "current_total_price_set", "current_total_tax_set", 
"subtotal_price_set", "tax_lines", "total_discounts_set", "total_line_items_price_set", "total_price_set",
"total_shipping_price_set", "total_tax_set", "discount_applications"
, "line_items", "payment_details", "refunds", "shipping_lines", "payment_terms","discount_codes" ],
    where = max_loaded_batchruntime) }}
)


select

b.* {{exclude()}} (_dbt_source_relation, {{daton_user_id()}}, {{daton_batch_runtime()}}, {{daton_batch_id()}}, row_num),
b.{{daton_user_id()}} as _daton_user_id,
b.{{daton_batch_runtime()}} as _daton_batch_runtime,
b.{{daton_batch_id()}} as _daton_batch_id,

current_timestamp() as _last_updated,
'{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id
from (
    select 
    fulfillments.id as id,
    fulfillments.created_at,
    fulfillments.location_id,
    fulfillments.name,
    fulfillments.order_id,
    receipt.authorization as receipt_authorization,
    fulfillments.service,
    fulfillments.shipment_status,
    fulfillments.status,
    fulfillments.tracking_company,
    -- fulfillments.tracking_number,
    fulfillments.tracking_numbers,
    fulfillments.tracking_urls,
    fulfillments.updated_at, 
    _dbt_source_relation,
    a.{{daton_user_id()}} as _daton_user_id,
    a.{{daton_batch_runtime()}} as _daton_batch_runtime,
    a.{{daton_batch_id()}} as _daton_batch_id,
    row_number() over (partition by a.id,fulfillments.id order by a.{{daton_batch_runtime()}} desc) as row_num
    from union_tables a
    {{unnesting("fulfillments")}}
    {{multi_unnesting('fulfillments','receipt')}}
    ) b
where row_num=1