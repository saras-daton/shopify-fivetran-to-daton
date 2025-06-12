{% if  var('KlaviyoFlowActions') %}
{{ config( enabled = True ) }}
{% else %}
{{ config( enabled = False ) }}
{% endif %}

{% if is_incremental() %}
    {% set max_loaded_batchruntime = '_daton_batch_runtime >= (select coalesce(max(_daton_batch_runtime),0) from ' ~ this ~ ')' %}
{% else %}
    {% set max_loaded_batchruntime = '1=1' %}
{% endif %}

{% set relations = dbt_utils.get_relations_by_pattern(
schema_pattern=var('raw_schema'),
table_pattern='%klaviyo%flow_actions',
database=var('raw_database')) %}

{% for i in relations %}
        select
        coalesce(id, 'NA') as id,
        coalesce(flow_id, 'NA') as flow_id,
        {{extract_nested_value("attributes","action_type","string")}} as action_type,
        {{extract_nested_value("attributes","created","timestamp")}} as created,
        {{extract_nested_value("render_options","add_info_link","BOOLEAN")}} as render_options_add_info_link,
        {{extract_nested_value("render_options","add_opt_out_language","BOOLEAN")}} as render_options_add_opt_out_language,
        {{extract_nested_value("render_options","add_org_prefix","BOOLEAN")}} as render_options_add_org_prefix,
        cast(null as string) as render_options_include_contact_card,
        {{extract_nested_value("render_options","shorten_links","BOOLEAN")}} as render_options_shorten_links,
        {{extract_nested_value("attributes","status","string")}} as status,
        {{extract_nested_value("send_options","is_transactional","BOOLEAN")}} as send_option_is_transactional,
        {{extract_nested_value("send_options","use_smart_sending","BOOLEAN")}} as send_option_use_smart_sending,
        {{extract_nested_value("tracking_options","add_utm","BOOLEAN")}} as tracking_options_is_add_utm,
        {{extract_nested_value("tracking_options","is_tracking_clicks","BOOLEAN")}} as tracking_options_is_tracking_clicks,
        {{extract_nested_value("tracking_options","is_tracking_opens","BOOLEAN")}} as tracking_options_is_tracking_opens,
        {{extract_nested_value("attributes","updated","timestamp")}} as updated,
        a.{{daton_user_id()}} as _daton_user_id,
        a.{{daton_batch_runtime()}} as _daton_batch_runtime,
        a.{{daton_batch_id()}} as _daton_batch_id,
        current_timestamp() as _last_updated,
        '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id
        from {{i}} a
        {{unnesting("attributes")}}
        {{multi_unnesting('attributes','render_options')}}
        {{multi_unnesting('attributes','send_options')}}
        {{multi_unnesting('attributes','tracking_options')}}

    {% if is_incremental() %}
        {# /* -- this filter will only be applied on an incremental run */ #}
        where {{max_loaded_batchruntime}}
    {% endif %}
    {% if not loop.last %} union all {% endif %}
{% endfor %}
qualify row_number() over (partition by id order by _daton_batch_runtime desc) = 1