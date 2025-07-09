{% if var('SHOPIFYV2', True) %}
  {{ config(enabled=True) }}
{% else %}
  {{ config(enabled=False) }}
{% endif %}

{% if is_incremental() %}
  {% set max_loaded_batchruntime = '_daton_batch_runtime >= (select coalesce(max(_daton_batch_runtime), 0) from ' ~ this ~ ')' %}
{% else %}
  {% set max_loaded_batchruntime = '1=1' %}
{% endif %}

{% set table_relations = dbt_utils.get_relations_by_pattern(
  schema_pattern=var('raw_schema'),
  table_pattern=var('shopify_products_tbl_ptrn', '%shopify%products'),
  exclude=var('shopify_products_exclude_tbl_ptrn', ''),
  database=var('raw_database')
) %}

with union_tables as (
  {{ dbt_utils.union_relations(
      relations=table_relations,
      where=max_loaded_batchruntime) }}
),

options_flattened as (
  select
    options.id as id,
    coalesce(a.id, 0) as product_option_id,
    options.name,

    a.{{ daton_user_id() }} as _daton_user_id,
    a.{{ daton_batch_runtime() }} as _daton_batch_runtime,
    a.{{ daton_batch_id() }} as _daton_batch_id,

    row_number() over (
      partition by coalesce(a.id, 0), options.id
      order by a.{{ daton_batch_runtime() }} desc
    ) as row_num

  from union_tables a
  {{ unnesting("options") }}
  where {{ max_loaded_batchruntime }}
)

select
  id,
  product_option_id,
  name,
  _daton_user_id,
  _daton_batch_runtime,
  _daton_batch_id,
  current_timestamp() as _last_updated,
  '{{ env_var("DBT_CLOUD_RUN_ID", "manual") }}' as _run_id

from options_flattened
where row_num = 1
