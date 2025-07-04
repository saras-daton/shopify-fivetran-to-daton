{% if var('SHOPIFYV2') and var('ShopifyOrders', true) %}
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
    relations=table_relations,
    exclude=[
      "discount_applications", "fulfillments", "line_items", "payment_details", 
      "refunds", "shipping_lines", "payment_terms", "discount_codes"
    ],
    where=max_loaded_batchruntime
  ) }}
)

select
  a.id,
  customer.id as customer_id,
  a.location_id,
  a.user_id,
  company.id as company_id,
  company.location_id as company_location_id,
  a.app_id,
  a.browser_ip,
  a.buyer_accepts_marketing,
  a.cancel_reason,
  {{ timezone_conversion("a.cancelled_at") }} as cancelled_at,
  {{ timezone_conversion("a.closed_at") }} as closed_at,
  a.cart_token,
  a.checkout_id,
  a.checkout_token,
  a.confirmed,
  {{ timezone_conversion("a.created_at") }} as created_at,
  a.currency,
  a.customer_locale,
  a.device_id,
  a.email,
  a.financial_status,
  a.fulfillment_status,
  a.landing_site as landing_site_base_url,
  a.landing_site_ref,
  a.name,
  a.note,
  note_attributes,
  a.number,
  a.order_number,
  a.order_status_url,
  client_details.user_agent as client_details_user_agent,
  a.current_subtotal_price,
  a.current_total_discounts,
  a.current_total_price,
  a.current_total_tax,
  a.payment_gateway_names,
  a.presentment_currency,
  {{ timezone_conversion("a.processed_at") }} as processed_at,
  a.reference,
  a.referring_site,
  a.source_identifier,
  a.source_name,
  a.source_url,
  a.subtotal_price,
  subtotal_price_set,
  a.taxes_included,
  a.test,
  a.total_discounts,
  total_discounts_set,
  a.total_line_items_price,
  total_line_items_price_set,
  a.total_price,
  total_price_set,
  total_shipping_price_set,
  a.total_tax,
  total_tax_set,
  a.total_tip_received,
  a.total_weight,
  {{ timezone_conversion("a.updated_at") }} as updated_at,
  billing_address,
  shipping_address,
  {{ daton_user_id() }} as _daton_user_id,
  {{ daton_batch_runtime() }} as _daton_batch_runtime,
  {{ daton_batch_id() }} as _daton_batch_id,
  current_timestamp() as _last_updated,
  '{{ env_var("DBT_CLOUD_RUN_ID", "manual") }}' as _run_id

from union_tables a
  left join unnest(a.customer) as customer
  left join unnest(a.note_attributes) as note_attributes
  left join unnest(a.billing_address) as billing_address
  left join unnest(a.shipping_address) as shipping_address
  left join unnest(a.client_details) as client_details
  left join unnest(a.subtotal_price_set) as subtotal_price_set
  left join unnest(a.total_discounts_set) as total_discounts_set
  left join unnest(a.total_line_items_price_set) as total_line_items_price_set
  left join unnest(a.total_price_set) as total_price_set
  left join unnest(a.total_shipping_price_set) as total_shipping_price_set
  left join unnest(a.total_tax_set) as total_tax_set
  left join unnest(a.company) as company

qualify row_number() over (
  partition by coalesce(a.id, 0) 
  order by {{ daton_batch_runtime() }} desc
) = 1
