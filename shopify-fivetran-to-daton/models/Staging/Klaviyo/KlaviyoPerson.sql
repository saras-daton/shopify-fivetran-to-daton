{% if  var('KlaviyoPerson') %}
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
table_pattern='%klaviyo%profiles',
database=var('raw_database')) %}

{% for i in relations %}
        select
        coalesce(a.id, 'NA') as id,
        {{extract_nested_value("location","address1","string")}} as address1,
        {{extract_nested_value("location","address2","string")}} as address2,
        {{extract_nested_value("location","city","string")}} as city,
        {{extract_nested_value("location","country","string")}} as country,
        {{extract_nested_value("attributes","created","timestamp")}} as created,
        {{extract_nested_value("attributes","email","string")}} as email,
        {{extract_nested_value("attributes","external_id","string")}} as external_id,
        {{extract_nested_value("attributes","image","string")}} as image,
        {{extract_nested_value("attributes","first_name","string")}} as first_name,
        {{extract_nested_value("attributes","last_name","string")}} as last_name,
        {{extract_nested_value("attributes","last_event_date","datetime")}} as last_event_date,
        {{extract_nested_value("location","latitude","string")}} as latitude,
        {{extract_nested_value("location","longitude","string")}} as longitude,
        {{extract_nested_value("attributes","organization","string")}} as organization,
        {{extract_nested_value("attributes","phone_number","string")}} as phone_number,
        {{extract_nested_value("predictive_analytics","average_days_between_orders","numeric")}} as predictive_analytics_average_days_between_orders,
        {{extract_nested_value("predictive_analytics","average_order_value","numeric")}} as predictive_analytics_average_order_value,
        {{extract_nested_value("predictive_analytics","churn_probability","numeric")}} as predictive_analytics_churn_probability,
        {{extract_nested_value("predictive_analytics","expected_date_of_next_order","timestamp")}} as predictive_analytics_expected_date_of_next_order,
        {{extract_nested_value("predictive_analytics","historic_clv","numeric")}} as predictive_analytics_historic_clv,
        {{extract_nested_value("predictive_analytics","historic_number_of_orders","numeric")}} as predictive_analytics_historic_number_of_orders,
        {{extract_nested_value("predictive_analytics","predicted_clv","numeric")}} as predictive_analytics_predicted_clv,
        {{extract_nested_value("predictive_analytics","predicted_number_of_orders","numeric")}} as predictive_analytics_predicted_number_of_orders,
        {{extract_nested_value("predictive_analytics","total_clv","numeric")}} as predictive_analytics_total_clv,
        {{extract_nested_value("location","region","string")}} as region,
        {{extract_nested_value("email_marketing","consent","string")}} as subscriptions_email_marketing_consent,
        {{extract_nested_value("email_marketing","custom_method_detail","string")}} as subscriptions_email_marketing_custom_method_detail,
        {{extract_nested_value("email_marketing","double_optin","BOOLEAN")}} as subscriptions_email_marketing_double_optin,
        {{extract_nested_value("email_marketing","method","string")}} as subscriptions_email_marketing_method,
        {{extract_nested_value("email_marketing","method_detail","string")}} as subscriptions_email_marketing_method_detail,
        {{extract_nested_value("email_marketing","consent_timestamp","timestamp")}} as subscriptions_email_marketing_timestamp,
        {{extract_nested_value("sms_marketing","consent","string")}} as subscriptions_sms_marketing_consent,
        {{extract_nested_value("sms_marketing","method","string")}} as subscriptions_sms_marketing_method,
        {{extract_nested_value("sms_marketing","method_detail","string")}} as subscriptions_sms_marketing_method_detail,
        {{extract_nested_value("sms_marketing","consent_timestamp","timestamp")}} as subscriptions_sms_marketing_timestamp,
        {{extract_nested_value("location","timezone","string")}} as timezone,
        {{extract_nested_value("attributes","title","string")}} as title,
        {{extract_nested_value("attributes","updated","timestamp")}} as updated,
        {{extract_nested_value("location","zip","string")}} as zip,
        a.{{daton_user_id()}} as _daton_user_id,
        a.{{daton_batch_runtime()}} as _daton_batch_runtime,
        a.{{daton_batch_id()}} as _daton_batch_id,
        current_timestamp() as _last_updated,
        '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id
        from {{i}} a
        {{unnesting("attributes")}}
        {{multi_unnesting('attributes','location')}}
        {{multi_unnesting('attributes','predictive_analytics')}}
        {{multi_unnesting('attributes','subscriptions')}}
        {{multi_unnesting('subscriptions','email')}}
        {{multi_unnesting('subscriptions','sms')}}
        left join unnest(email.marketing) email_marketing 
        left join unnest(sms.marketing) sms_marketing 

    {% if is_incremental() %}
        {# /* -- this filter will only be applied on an incremental run */ #}
        where {{max_loaded_batchruntime}}
    {% endif %}
    {% if not loop.last %} union all {% endif %}
{% endfor %}
qualify row_number() over (partition by id order by _daton_batch_runtime desc) = 1