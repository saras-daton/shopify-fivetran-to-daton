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
  table_pattern=var('shopify_fulfillment_orders_tbl_ptrn', '%shopify%fulfillment_orders'),
  exclude=var('shopify_fulfillment_orders_exclude_tbl_ptrn', ''),
  database=var('raw_database')) %}

with union_tables as (
  {{ dbt_utils.union_relations(
    relations = table_relations,
    where = max_loaded_batchruntime
  ) }}
)

select
  b.id,
  b.order_id,
  b.created_at,
  b.fulfill_at,
  b.fulfill_by,
  b.international_duties_incoterm,
  b.request_status,
  b.status,
  b.supported_actions,
  b.updated_at,

  b.assigned_location_country_code,
  b.assigned_location_location_id,
  b.assigned_location_name,
  b.assigned_location_address1,
  b.assigned_location_address2,
  b.assigned_location_city,
  b.assigned_location_phone,
  b.assigned_location_province,
  b.assigned_location_zip,

  b.delivery_method_id,
  b.delivery_method_method_type,
  b.delivery_method_min_delivery_date_time,
  b.delivery_method_max_delivery_date_time,

  b.destination_id,
  b.destination_address1,
  b.destination_address2,
  b.destination_city,
  b.destination_country,
  b.destination_email,
  b.destination_first_name,
  b.destination_last_name,
  b.destination_phone,
  b.destination_province,
  b.destination_zip,
  b.destination_company,

  b.{{ daton_user_id() }} as _daton_user_id,
  b.{{ daton_batch_runtime() }} as _daton_batch_runtime,
  b.{{ daton_batch_id() }} as _daton_batch_id,
  current_timestamp() as _last_updated,
  '{{ env_var("DBT_CLOUD_RUN_ID", "manual") }}' as _run_id

from (
  select
    a.id,
    a.order_id,
    {{ timezone_conversion("created_at") }} as created_at,
    {{ timezone_conversion("fulfill_at") }} as fulfill_at,
    {{ timezone_conversion("fulfill_by") }} as fulfill_by,
    {{ timezone_conversion("updated_at") }} as updated_at,
    a.request_status,
    a.status,
    a.supported_actions,
    a.international_duties[SAFE_OFFSET(0)].incoterm as international_duties_incoterm,

    -- Flatten assigned_location[0] safely
    a.assigned_location[SAFE_OFFSET(0)].country_code as assigned_location_country_code,
    a.assigned_location[SAFE_OFFSET(0)].location_id as assigned_location_location_id,
    a.assigned_location[SAFE_OFFSET(0)].name as assigned_location_name,
    a.assigned_location[SAFE_OFFSET(0)].address1 as assigned_location_address1,
    a.assigned_location[SAFE_OFFSET(0)].address2 as assigned_location_address2,
    a.assigned_location[SAFE_OFFSET(0)].city as assigned_location_city,
    a.assigned_location[SAFE_OFFSET(0)].phone as assigned_location_phone,
    a.assigned_location[SAFE_OFFSET(0)].province as assigned_location_province,
    a.assigned_location[SAFE_OFFSET(0)].zip as assigned_location_zip,

    -- Flatten delivery_method[0] safely
    a.delivery_method[SAFE_OFFSET(0)].id as delivery_method_id,
    a.delivery_method[SAFE_OFFSET(0)].method_type as delivery_method_method_type,
    a.delivery_method[SAFE_OFFSET(0)].min_delivery_date_time as delivery_method_min_delivery_date_time,
    a.delivery_method[SAFE_OFFSET(0)].max_delivery_date_time as delivery_method_max_delivery_date_time,

    -- Flatten destination[0] safely
    a.destination[SAFE_OFFSET(0)].id as destination_id,
    a.destination[SAFE_OFFSET(0)].address1 as destination_address1,
    a.destination[SAFE_OFFSET(0)].address2 as destination_address2,
    a.destination[SAFE_OFFSET(0)].city as destination_city,
    a.destination[SAFE_OFFSET(0)].country as destination_country,
    a.destination[SAFE_OFFSET(0)].email as destination_email,
    a.destination[SAFE_OFFSET(0)].first_name as destination_first_name,
    a.destination[SAFE_OFFSET(0)].last_name as destination_last_name,
    a.destination[SAFE_OFFSET(0)].phone as destination_phone,
    a.destination[SAFE_OFFSET(0)].province as destination_province,
    a.destination[SAFE_OFFSET(0)].zip as destination_zip,
    a.destination[SAFE_OFFSET(0)].company as destination_company,

    {{ daton_user_id() }},
    {{ daton_batch_runtime() }},
    {{ daton_batch_id() }},
    
    row_number() over (partition by a.id order by a.{{ daton_batch_runtime() }} desc) as row_num

  from union_tables a
) b
where row_num = 1
