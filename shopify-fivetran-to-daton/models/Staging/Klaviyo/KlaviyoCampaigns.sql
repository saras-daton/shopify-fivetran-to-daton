{% if  var('KlaviyoCampaigns') %}
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
table_pattern='%klaviyo%campaigns',
database=var('raw_database')) %}

{% for i in relations %}
        select
        coalesce(a.id, 'NA') as id,
        {{extract_nested_value("attributes","archived","BOOLEAN")}} as archived,
        {{extract_nested_value("attributes","created_at","timestamp")}} as created,  
        {{extract_nested_value("attributes","name","string")}} as name, 
        {{extract_nested_value("attributes","scheduled_at","timestamp")}} as scheduled,   
        {{extract_nested_value("send_options","ignore_unsubscribes","BOOLEAN")}} as send_option_ignore_unsubscribes,
        {{extract_nested_value("send_options","use_smart_sending","BOOLEAN")}} as send_option_use_smart_sending,
        {{extract_nested_value("send_strategy","method","string")}} as send_strategy_method,
        cast(null as timestamp) as send_strategy_options_static_datetime,
        cast(null as BOOLEAN) as send_strategy_options_static_is_local,
        cast(null as BOOLEAN) as send_strategy_options_static_send_past_recipients_immediately,
        cast(null as date) as send_strategy_options_sto_date,
        cast(null as timestamp) as send_strategy_options_throttled_datetime,
        cast(null as FLOAT64) as send_strategy_options_throttled_throttle_percentage,
        {{extract_nested_value("attributes","send_time","timestamp")}} as send_time,  
        {{extract_nested_value("attributes","status","string")}} as status,   	   
        {{extract_nested_value("tracking_options","is_add_utm","BOOLEAN")}} as tracking_options_is_add_utm,   	   
        {{extract_nested_value("tracking_options","is_tracking_clicks","BOOLEAN")}} as tracking_options_is_tracking_clicks,   	   
        {{extract_nested_value("tracking_options","is_tracking_opens","BOOLEAN")}} as tracking_options_is_tracking_opens,
        {{extract_nested_value("attributes","updated_at","timestamp")}} as updated,    	   	   		   	
        a.{{daton_user_id()}} as _daton_user_id,
        a.{{daton_batch_runtime()}} as _daton_batch_runtime,
        a.{{daton_batch_id()}} as _daton_batch_id,
        current_timestamp() as _last_updated,
        '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id
        from {{i}} a
        {{unnesting("attributes")}}
        {{multi_unnesting('attributes','send_options')}}
        {{multi_unnesting('attributes','send_strategy')}}
        {{multi_unnesting('attributes','tracking_options')}}

    {% if is_incremental() %}
        {# /* -- this filter will only be applied on an incremental run */ #}
        where {{max_loaded_batchruntime}}
    {% endif %}
    {% if not loop.last %} union all {% endif %}
{% endfor %}
qualify row_number() over (partition by id order by _daton_batch_runtime desc) = 1