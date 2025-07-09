{% if  var('KlaviyoFlowMessages') %}
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
table_pattern='%klaviyo%flow_messages',
database=var('raw_database')) %}

{% for i in relations %}
        select
        coalesce(a.id, 'NA') as id,
        coalesce(flow_action_id, 'NA') as flow_action_id,
        {{extract_nested_value("data","id","string")}} as template_id,
        {{extract_nested_value("attributes","channel","string")}} as channel,
        {# /* {{extract_nested_value("content","body","string")}} as content_body, */ #}
        cast(null as string) as content_body,
        {{extract_nested_value("content","cc_email","string")}} as content_cc_email,
        {{extract_nested_value("content","from_email ","string")}} as content_from_email,
        {{extract_nested_value("content","from_label","string")}} as content_from_label,
        {# /* {{extract_nested_value("content","media_url ","string")}} as content_media_url, */ #}
        cast(null as string) as content_media_url,
        {{extract_nested_value("content","preview_text ","string")}} as content_preview_text,
        {{extract_nested_value("content","reply_to_email","string")}} as content_reply_to_email,
        {{extract_nested_value("content","subject","string")}} as content_subject,
        {{extract_nested_value("attributes","created","timestamp")}} as created,
        {{extract_nested_value("attributes","name","string")}} as name,
        {{extract_nested_value("attributes","updated","timestamp")}} as updated,
        a.{{daton_user_id()}} as _daton_user_id,
        a.{{daton_batch_runtime()}} as _daton_batch_runtime,
        a.{{daton_batch_id()}} as _daton_batch_id,
        current_timestamp() as _last_updated,
        '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id
        from {{i}} a
        {{unnesting("attributes")}}
        {{unnesting("relationships")}}
        {{multi_unnesting('relationships','template')}}
        {{multi_unnesting('template','data')}}
        {{multi_unnesting('attributes','content')}}
    {% if is_incremental() %}
        {# /* -- this filter will only be applied on an incremental run */ #}
        where {{max_loaded_batchruntime}}
    {% endif %}
    {% if not loop.last %} union all {% endif %}
{% endfor %}
qualify row_number() over (partition by id order by _daton_batch_runtime desc) = 1