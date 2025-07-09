    {% if var('SHOPIFYV2') %}
    {{ config(enabled=True) }}
    {% else %}
    {{ config(enabled=False) }}
    {% endif %}

    {% if is_incremental() %}
    {% set max_loaded_batchruntime = '_daton_batch_runtime >= (select coalesce(max(_daton_batch_runtime),0) from ' ~ this ~ ')' %}
    {% else %}
    {% set max_loaded_batchruntime = '1=1' %}
    {% endif %}

    {% set table_relations = dbt_utils.get_relations_by_pattern(
    schema_pattern=var('raw_schema'),
    table_pattern=var('shopify_price_rules_tbl_ptrn','%shopify_price_rule%'),
    exclude=var('shopify_price_rules_exclude_tbl_ptrn',''),
    database=var('raw_database')) %}

    with union_tables as (
    {{ dbt_utils.union_relations(
        relations = table_relations,
        where = max_loaded_batchruntime
    ) }}
    )

    select
    replace(split(split(_dbt_source_relation,'.')[2],'_')[0],'`','') as brand,

    b.id,
    b.usage_limit,
    b.allocation_method,
    b.created_at,
    b.customer_selection,
    b.ends_at,
    b.once_per_customer,
    b.starts_at,
    b.target_selection,
    b.target_type,
    b.title,
    b.updated_at,
    b.value,
    b.value_type,

    -- Flattened repeated RECORDs
    b.prerequisite_quantity_range_greater_than_or_equal_to as prerequisite_quantity_range,
    b.prerequisite_shipping_price_range_less_than_or_equal_to as prerequisite_shipping_price_range,
    b.prerequisite_subtotal_range_greater_than_or_equal_to as prerequisite_subtotal_range,
    b.prerequisite_to_entitlement_quantity_ratio_entitled_quantity as quantity_ratio_entitled_quantity,
    b.prerequisite_to_entitlement_quantity_ratio_prerequisite_quantity as quantity_ratio_prerequisite_quantity,

    -- Metadata
    b.{{ daton_user_id() }} as _daton_user_id,
    b.{{ daton_batch_runtime() }} as _daton_batch_runtime,
    b.{{ daton_batch_id() }} as _daton_batch_id,
    _dbt_source_relation,
    concat(split(lower(replace(split(_dbt_source_relation,'.')[2],'`','')),'_shopify')[0], '_', REGEXP_REPLACE(replace(split(_dbt_source_relation,'.')[2],'`',''),'[^0-9]','')) as _daton_sourceversion_integration_id,
    current_timestamp() as _last_updated,
    '{{ env_var("DBT_CLOUD_RUN_ID", "manual") }}' as _run_id

    from (
    select
        a.id,
        a.starts_at,
        a.ends_at,
        a.created_at,
        a.updated_at,

        -- Flatten nested arrays (safe with OFFSET(0))
        a.prerequisite_quantity_range[OFFSET(0)].greater_than_or_equal_to as prerequisite_quantity_range_greater_than_or_equal_to,
        a.prerequisite_shipping_price_range[OFFSET(0)].less_than_or_equal_to as prerequisite_shipping_price_range_less_than_or_equal_to,
        a.prerequisite_subtotal_range[OFFSET(0)].greater_than_or_equal_to as prerequisite_subtotal_range_greater_than_or_equal_to,
        a.prerequisite_to_entitlement_quantity_ratio[OFFSET(0)].entitled_quantity as prerequisite_to_entitlement_quantity_ratio_entitled_quantity,
        a.prerequisite_to_entitlement_quantity_ratio[OFFSET(0)].prerequisite_quantity as prerequisite_to_entitlement_quantity_ratio_prerequisite_quantity,

        a.* except(
        id, starts_at, ends_at, created_at, updated_at,
        prerequisite_quantity_range,
        prerequisite_shipping_price_range,
        prerequisite_subtotal_range,
        prerequisite_to_entitlement_quantity_ratio
        ),

        dense_rank() over (partition by a.id order by a.{{ daton_batch_runtime() }} desc) as row_num

    from union_tables a
    ) b
    where row_num = 1
