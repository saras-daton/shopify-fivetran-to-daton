{% if var('ShopifyProductVariant', true) %}
  {{ config(enabled = true) }}
{% else %}
  {{ config(enabled = false) }}
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
      relations = table_relations,
      where = max_loaded_batchruntime
  ) }}
)

select
 
  variant.id as id,
  parent.id as product_id,
  variant.inventory_item_id,
  variant.image_id,
--   variant.available_for_sale,
  variant.barcode,
  variant.compare_at_price,
  variant.created_at,
--   variant.display_name,
  variant.inventory_policy,
  variant.inventory_quantity,
--   variant.legacy_resource_id,
  variant.position,
  variant.price,
--   variant.requires_components,
--   variant.sellable_online_quantity,
  variant.sku,
  variant.tax_code,
  variant.taxable,
  variant.title,
  variant.updated_at,

  -- audit fields
  {{ daton_user_id() }} as datonuser_id,
  {{ daton_batch_runtime() }} as datonbatch_runtime,
  {{ daton_batch_id() }} as datonbatch_id,

  current_timestamp() as lastupdated,
  '{{ env_var("DBT_CLOUD_RUN_ID", "manual") }}' as runid

from (
  select
    *,
    dense_rank() over (
      partition by coalesce(id, 0)
      order by {{ daton_batch_runtime() }} desc
    ) as row_num
  from union_tables
) as parent,
unnest(parent.variants) as variant
where row_num = 1
