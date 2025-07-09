{% if var('SHOPIFYV2') and var('ShopifyTransactions', True) %}
  {{ config(enabled = True) }}
{% else %}
  {{ config(enabled = False) }}
{% endif %}

{% if is_incremental() %}
  {% set max_loaded_batchruntime = '_daton_batch_runtime >= (select coalesce(max(_daton_batch_runtime),0) from ' ~ this ~ ')' %}
{% else %}
  {% set max_loaded_batchruntime = '1=1' %}
{% endif %}

{% set table_relations = dbt_utils.get_relations_by_pattern(
  schema_pattern=var('raw_schema'),
  table_pattern=var('shopify_transactions_tbl_ptrn', '%shopify%transactions'),
  exclude=var('shopify_transactions_exclude_tbl_ptrn', '%tender%'),
  database=var('raw_database')
) %}

with union_tables as (
  {{ dbt_utils.union_relations(
    relations = table_relations,
    exclude = ["total_unsettled_set", "payments_refund_attributes", "receipt.charges",
               "receipt.metadata", "receipt.paymentinfo", "receipt.last_payment_error",
               "receipt.payment_method_details", "receipt.error", "receipt.balance_transaction",
               "receipt.transaction_event", "receipt.refund_info", "signature"],
    where = max_loaded_batchruntime
  ) }}
),

flat as (
  select
    coalesce(a.id, 0) as id,
    a.order_id,
    a.parent_id,
    a.location_id,
    a.amount,
    a.authorization,
    -- null as authorization_expires_at, -- not in raw
    created_at,
    a.currency,
    -- null as currency_exchange_id,
    -- null as currency_exchange_adjustment,
    -- null as currency_exchange_original_amount,
    -- null as currency_exchange_final_amount,
    -- null as currency_exchange_currency,
    a.device_id,
    a.error_code,
    -- null as extended_authorization_attributes_extended_authorization_expires_at,
    a.gateway,
    a.kind,
    a.message,
    pd.avs_result_code as payment_avs_result_code,
    pd.credit_card_bin as payment_credit_card_bin,
    pd.cvv_result_code as payment_cvv_result_code,
    pd.credit_card_number as payment_credit_card_number,
    pd.credit_card_company as payment_credit_card_company,
    {{ timezone_conversion("a.processed_at") }} as processed_at,
    r.refund_id,
    a.source_name,
    a.status,
    a.test,
    a.user_id,
    -- null as standard_authorization_expires_at,

    a.{{ daton_user_id() }} as _daton_user_id,
    a.{{ daton_batch_runtime() }} as _daton_batch_runtime,
    a.{{ daton_batch_id() }} as _daton_batch_id,

    row_number() over (
      partition by coalesce(a.id, 0)
      order by a.{{ daton_batch_runtime() }} desc
    ) as row_num

  from union_tables a
  left join unnest(a.receipt) as r
  left join unnest(a.payment_details) as pd
)

select
  id,
  order_id,
  refund_id,
  location_id,
  parent_id,
  amount,
  authorization,
--   authorization_expires_at,
  created_at,
  currency,
--   currency_exchange_id,
--   currency_exchange_adjustment,
--   currency_exchange_original_amount,
--   currency_exchange_final_amount,
--   currency_exchange_currency,
  device_id,
  error_code,
--   extended_authorization_attributes_extended_authorization_expires_at,
  gateway,
  kind,
  message,
  payment_avs_result_code,
  payment_credit_card_bin,
  payment_cvv_result_code,
  payment_credit_card_number,
  payment_credit_card_company,
  processed_at,
  source_name,
  status,
  test,
  user_id,
--   standard_authorization_expires_at,
  _daton_user_id,
  _daton_batch_runtime,
  _daton_batch_id,
  current_timestamp() as _last_updated,
  '{{ env_var("DBT_CLOUD_RUN_ID", "manual") }}' as _run_id
from flat
where row_num = 1
