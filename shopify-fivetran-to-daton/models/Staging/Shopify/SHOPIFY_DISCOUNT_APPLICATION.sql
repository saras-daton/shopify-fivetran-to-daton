    {% if var('SHOPIFYV2')  %}
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
        table_pattern=var('shopify_orders_tbl_ptrn','%shopify%orders'),
        exclude=var('shopify_orders_exclude_tbl_ptrn','%shopify%t_orders'),
        database=var('raw_database')
    ) %}

    with union_tables as (
        {{ dbt_utils.union_relations(
            relations = table_relations,
            exclude = [
                "client_details", "current_subtotal_price_set", "current_total_discounts_set",
                "current_total_price_set", "current_total_tax_set", "note_attributes",
                "subtotal_price_set", "tax_lines", "total_discounts_set", "total_line_items_price_set",
                "total_price_set", "total_shipping_price_set", "total_tax_set", "billing_address",
                "shipping_address", "customer", "discount_codes", "fulfillments",
                "payment_details", "refunds", "shipping_lines", "payment_terms"
            ],
            where = max_loaded_batchruntime
        ) }}
    )

    select
    coalesce(a.id, 0) as order_id,
    row_number() over (partition by coalesce(a.id, 0) order by _daton_batch_runtime desc)-1 index,
    discount_application.allocation_method,
    discount_application.code,
    discount_application.description,
    discount_application.target_selection,
    coalesce(discount_application.target_type, 'N/A') as target_type,
    discount_application.title,
    coalesce(discount_application.type, 'N/A') as type,
    discount_application.value,
    coalesce(discount_application.value_type, 'N/A') as value_type,
    a.{{ daton_user_id() }} as _daton_user_id,
    a.{{ daton_batch_runtime() }} as _daton_batch_runtime,
    a.{{ daton_batch_id() }} as _daton_batch_id,
    current_timestamp() as _last_updated,
    '{{ env_var("DBT_CLOUD_RUN_ID", "manual") }}' as _run_id

    from union_tables a

    -- unnest line_items first
    left join unnest(a.line_items) as line_item

   

    -- unnest discount_applications (at order level)
   left join unnest(a.discount_applications)  as discount_application


    qualify dense_rank() over (
    partition by coalesce(a.id, 0)
    order by a.{{ daton_batch_runtime() }} desc
    ) = 1
