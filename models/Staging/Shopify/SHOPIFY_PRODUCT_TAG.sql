{% if var('SHOPIFYV2', True) %}
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
    schema_pattern = var('raw_schema'),
    table_pattern = var('shopify_products_tbl_ptrn', '%shopify%products'),
    exclude = var('shopify_products_exclude_tbl_ptrn', ''),
    database = var('raw_database')
) %}

{% for i in table_relations %}
    select
        offset_number + 1 as index,
        cast(a.id as numeric) as product_id,
        trim(tag) as value,
        a._daton_user_id,
        a._daton_batch_runtime,
        a._daton_batch_id,
        current_timestamp() as _last_updated,
        '{{ env_var("DBT_CLOUD_RUN_ID", "manual") }}' as _run_id
    from {{ i }} a,
    unnest(split(a.tags, ',')) as tag with offset as offset_number
    {% if is_incremental() %}
    where {{ max_loaded_batchruntime }}
    {% endif %}
    {% if not loop.last %}union all{% endif %}
{% endfor %}

qualify row_number() over (
    partition by product_id, value 
    order by _daton_batch_runtime desc
) = 1
