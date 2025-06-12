-- depends_on: {{ ref('KlaviyoCampaigns') }}
{% if  var('KlaviyoEvents') and var('KlaviyoCampaigns') %}
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
table_pattern='%klaviyo%events',
database=var('raw_database')) %}

{% for i in relations %}
        select
        coalesce(a.id, 'NA') as id,
        c.id as Campaign_id,
        {{extract_nested_value("event_properties","flow","string")}} as flow_id,
        {{extract_nested_value("event_properties","message","string")}} as flow_message_id,
        {{extract_nested_value("metric_data","id","string")}} as metric_id,
        {{extract_nested_value("profile_data","id","string")}} as person_id,
        {{extract_nested_value("attributes","datetime","string")}} as datetime,
        {{extract_nested_value("attributes","timestamp","numeric")}} as timestamp,
        a.type as type,
        {{extract_nested_value("attributes","uuid","string")}} as uuid,
        event_properties.* except(CampaignName, flow, message),
        a.{{daton_user_id()}} as _daton_user_id,
        a.{{daton_batch_runtime()}} as _daton_batch_runtime,
        a.{{daton_batch_id()}} as _daton_batch_id,
        current_timestamp() as _last_updated,
        '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id
        from {{i}} a
        {{unnesting("attributes")}}
        {{unnesting("relationships")}}
        {{multi_unnesting('attributes','event_properties')}}
        {{multi_unnesting('relationships','profile')}}
        {{multi_unnesting('relationships','metric')}}
        left join unnest(profile.data) profile_data 
        left join unnest(metric.data) metric_data
        left join {{ref('KlaviyoCampaigns')}} c on event_properties.CampaignName = c.name
    {% if is_incremental() %}
        {# /* -- this filter will only be applied on an incremental run */ #}
        where {{max_loaded_batchruntime}}
    {% endif %}
    {% if not loop.last %} union all {% endif %}
{% endfor %}
qualify row_number() over (partition by id order by _daton_batch_runtime desc) = 1