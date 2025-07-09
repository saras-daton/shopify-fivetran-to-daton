{% if var('SHOPIFYV2') and var('ShopifyProducts',True) %}
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
  table_pattern=var('shopify_products_tbl_ptrn','%shopify%products'),
  exclude=var('shopify_products_exclude_tbl_ptrn',''),
  database=var('raw_database')) %}

with union_tables as (
  {{ dbt_utils.union_relations(
      relations = table_relations,
      where = max_loaded_batchruntime) }}
),

exploded as (
  select
    a.* except(variants, options, images, image),
    image.id as featured_media_id,
    variant.inventory_quantity,
    cast(p.amount as float64) as price_amount,
    p.currency_code,
    dense_rank() over (partition by a.id order by a.{{ daton_batch_runtime() }} desc) as row_num
  from union_tables a
  left join unnest(a.image) as image
  left join unnest(a.variants) as variant
  left join unnest(variant.presentment_prices) as pp
  left join unnest(pp.price) as p
)

select
--   replace(split(split(_dbt_source_relation,'.')[2],'_')[0],'`','') as brand,

  -- Product-level fields
  b.id,
  b.featured_media_id,
  b.created_at,
  b.body_html as description_html,
  b.body_html as description,
  b.template_suffix as gift_card_template_suffix,
  b.handle,
--   false as has_only_default_variant,
--   false as has_out_of_stock_variants,
--   false as has_variants_that_requires_components,
--   false as is_gift_card,
  b.admin_graphql_api_id as legacy_resource_id,
  null as online_store_preview_url,
  max(b.price_amount) as max_variant_price_amount,
  min(b.price_amount) as min_variant_price_amount,
  any_value(b.currency_code) as max_variant_price_currency_code,
  any_value(b.currency_code) as min_variant_price_currency_code,
  b.product_type,
  b.published_at,
  false as requires_selling_plan,
--   null as seo_description,
--   null as seo_title,
  b.status,
  b.template_suffix,
  b.title,
  sum(b.inventory_quantity) as total_inventory,
  true as tracks_inventory,
  b.updated_at,
  b.vendor,
--   false as _fivetran_deleted,
--   null as compare_at_price_range_min_amount,
--   null as compare_at_price_range_max_amount,
  b.{{ daton_user_id() }} as _daton_user_id,
  b.{{ daton_batch_runtime() }} as _daton_batch_runtime,
  b.{{ daton_batch_id() }} as _daton_batch_id,
--   _dbt_source_relation,
--   concat(
--     split(lower(replace(split(_dbt_source_relation,'.')[2],'`','')),'_shopify')[0],
--     '_',
--     regexp_replace(replace(split(_dbt_source_relation,'.')[2],'`',''),'[^0-9]','')
--   ) as _daton_sourceversion_integration_id,
  current_timestamp() as _last_updated,
  '{{ env_var("DBT_CLOUD_RUN_ID", "manual") }}' as _run_id

from exploded b
where row_num = 1
group by
   b.id, b.featured_media_id, b.created_at, b.body_html, b.template_suffix, b.handle,
  b.admin_graphql_api_id, b.product_type, b.published_at, b.status,
  b.title, b.updated_at, b.vendor, b.{{ daton_user_id() }}, b.{{ daton_batch_runtime() }},
  b.{{ daton_batch_id() }}, _dbt_source_relation
