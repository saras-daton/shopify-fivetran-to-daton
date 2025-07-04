{% if var('ShopifyDiscountCodes',True) %}
{{ config( enabled = True ) }}
{% else %}
{{ config( enabled = False ) }}
{% endif %}

{% if is_incremental() %}
    {% set max_loaded_batchruntime = '_daton_batch_runtime >= (select coalesce(max(_daton_batch_runtime),0) from ' ~ this ~ ')' %}
{% else %}
    {% set max_loaded_batchruntime = '1=1' %}
{% endif %}

{% set all_tables = dbt_utils.get_relations_by_pattern(
    schema_pattern=var('raw_schema'),
    table_pattern=var('shopify_orders_tbl_ptrn', '%shopify%discount_codes'),
    database=var('mdl_database')
) %}


{% set table_relations = all_tables | selectattr("identifier", "in", required_tables) | list %}

with union_tables as (
{{ dbt_utils.union_relations(
    relations = table_relations,
    exclude = ["client_details", "current_subtotal_price_set", 
"current_total_discounts_set", "current_total_price_set", "current_total_tax_set", "note_attributes", 
"subtotal_price_set", "tax_lines", "total_discounts_set", "total_line_items_price_set", "total_price_set",
"total_shipping_price_set", "total_tax_set", "billing_address", "shipping_address", "customer", "discount_codes",
"fulfillments", "payment_details", "refunds", "shipping_lines", "payment_terms", "discount_applications"],
    where = max_loaded_batchruntime) }}
)

select 
replace(split(split(_dbt_source_relation,'.')[2],'_')[0],'`','') as brand,
replace(split(split(_dbt_source_relation, '.')[2], '_')[1], '`', '') as store,
REGEXP_EXTRACT(id, r'[^/]+$') AS discount_id,			
discountClass discount_class,
status,	
startsat,			
endsat,	
appliesOncepercustomer as once_per_customer,
summary, 
title as promo_code	,
_dbt_source_relation,
a.{{daton_user_id()}} as _daton_user_id,
a.{{daton_batch_runtime()}} as _daton_batch_runtime,
a.{{daton_batch_id()}} as _daton_batch_id,
concat(split(lower(replace(split(_dbt_source_relation,'.')[2],'`','')),'_shopify')[0], '_', REGEXP_REPLACE(replace(split(_dbt_source_relation,'.')[2],'`',''),'[^0-9]','')) as _daton_sourceversion_integration_id,
current_timestamp() as _last_updated,
'{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id
from union_tables a
{% if var('currency_conversion_flag') %}
    left join {{ref('ExchangeRates')}} b on date(created_at) = b.date and currency = b.to_currency_code
{% endif %}
qualify dense_rank() over (partition by id order by a.{{daton_batch_runtime()}} desc) = 1