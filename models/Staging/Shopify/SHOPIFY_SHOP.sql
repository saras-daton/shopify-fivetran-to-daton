{% if var('SHOPIFYV2')  %}
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
  table_pattern=var('shopify_shop_tbl_ptrn', '%shopify%shop'),
  exclude=var('shopify_shop_exclude_tbl_ptrn', ''),
  database=var('raw_database')) %}

with union_tables as (
  {{ dbt_utils.union_relations(
    relations = table_relations,
    where = max_loaded_batchruntime
  ) }}
)

select
  a.id,
  a.address1,
  a.address2,
--   a.auto_configure_tax_inclusivity,
  a.checkout_api_supported,
  a.city,
  a.cookie_consent_level,
  a.country,
  a.country_code,
  a.country_name,
  a.county_taxes,
  a.created_at,
  a.currency,
  a.customer_email,
  a.domain,
--   a.eligible_for_card_reader_giveaway,
  a.eligible_for_payments,
  a.email,
  a.enabled_presentment_currencies,
--   a.force_ssl,
--   a.google_apps_domain,
--   a.google_apps_login_enabled,
  a.has_discounts,
  a.has_gift_cards,
  a.has_storefront,
  a.iana_timezone,
  a.latitude,
  a.longitude,
  a.multi_location_enabled,
  a.myshopify_domain,
  a.name,
  a.password_enabled,
  a.phone,
  a.plan_display_name,
  a.plan_name,
  a.pre_launch_enabled,
  a.primary_locale,
  a.primary_location_id,
  a.province,
  a.province_code,
  a.requires_extra_payments_agreement,
  a.setup_required,
  a.shop_owner,
--   a.source,
--   a.tax_shipping,
  a.taxes_included,
  a.timezone,
  a.updated_at,
  a.visitor_tracking_consent_preference,
  a.weight_unit,
  a.zip,

  -- All money_* fields
  a.money_format,
  a.money_with_currency_format,
  a.money_in_emails_format,
  a.money_with_currency_in_emails_format,

  a.{{ daton_user_id() }} as _daton_user_id,
  a.{{ daton_batch_runtime() }} as _daton_batch_runtime,
  a.{{ daton_batch_id() }} as _daton_batch_id,
  current_timestamp() as _last_updated,
  '{{ env_var("DBT_CLOUD_RUN_ID", "manual") }}' as _run_id

from union_tables a
qualify dense_rank() over (partition by a.id order by a.{{ daton_batch_runtime() }} desc) = 1
