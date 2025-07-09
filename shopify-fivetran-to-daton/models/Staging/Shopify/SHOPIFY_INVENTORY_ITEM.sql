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
  table_pattern=var('shopify_inventory_items_tbl_ptrn', '%shopify%inventory_items'),
  exclude=var('shopify_inventory_items_exclude_tbl_ptrn', ''),
  database=var('raw_database')
) %}

with union_tables as (
  {{ dbt_utils.union_relations(
    relations = table_relations,
    where = max_loaded_batchruntime
  ) }}
)

select
--   replace(split(split(_dbt_source_relation, '.')[2], '_')[0], '`', '') as brand,

  -- Fields present in the schema
  b.id,
  b.created_at,
  b.updated_at,
  b.requires_shipping,
  b.sku,
  b.tracked,
  b.admin_graphql_api_id as legacy_resource_id,
  b.cost as unit_cost_amount,

  -- Skipped fields not found in schema:
  -- country_code_of_origin,
  -- duplicate_sku_count,
  -- harmonized_system_code,
  -- inventory_history_url,
  -- measurement_id,
  -- measurement_weight_value,
  -- measurement_weight_unit,
  -- province_code_of_origin,
  -- tracked_editable_locked,
  -- tracked_editable_reason,
  -- unit_cost_currency_code

  -- Metadata
  b.{{ daton_user_id() }} as _daton_user_id,
  b.{{ daton_batch_runtime() }} as _daton_batch_runtime,
  b.{{ daton_batch_id() }} as _daton_batch_id,
--   _dbt_source_relation,
--   concat(
--     split(lower(replace(split(_dbt_source_relation, '.')[2], '`', '')), '_shopify')[0],
--     '_',
--     REGEXP_REPLACE(replace(split(_dbt_source_relation, '.')[2], '`', ''), '[^0-9]', '')
--   ) as _daton_sourceversion_integration_id,
  current_timestamp() as _last_updated,
  '{{ env_var("DBT_CLOUD_RUN_ID", "manual") }}' as _run_id

from (
  select
    coalesce(a.id, 0) as id,
    a.created_at,
    a.updated_at,
    a.* except(id, created_at, updated_at),
    row_number() over (partition by coalesce(a.id, 0) order by a.{{ daton_batch_runtime() }} desc) as row_num
  from union_tables a
) b
where row_num = 1
