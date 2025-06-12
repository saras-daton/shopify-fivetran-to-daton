{% if  var('KlaviyoCampaignTrackingUtmParam') %}
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
        coalesce(a.id, 'NA') as campaign_id,
        {{extract_nested_value("utm_params","name","string")}} as name,
        {{extract_nested_value("utm_params","name","string")}} as value,
        a.{{daton_user_id()}} as _daton_user_id,
        a.{{daton_batch_runtime()}} as _daton_batch_runtime,
        a.{{daton_batch_id()}} as _daton_batch_id,
        current_timestamp() as _last_updated,
        '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id
        from {{i}} a
        {{unnesting("attributes")}}
        {{multi_unnesting('attributes','tracking_options')}}
        {{multi_unnesting('tracking_options','utm_params')}}

    {% if is_incremental() %}
        {# /* -- this filter will only be applied on an incremental run */ #}
        where {{max_loaded_batchruntime}}
    {% endif %}
    {% if not loop.last %} union all {% endif %}
{% endfor %}
qualify row_number() over (partition by id order by _daton_batch_runtime desc) = 1