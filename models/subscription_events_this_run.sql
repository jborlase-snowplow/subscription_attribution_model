{{
  config(
    materialized='table',
    on_schema_change='append_new_columns',
    unique_key='cv_id',
    upsert_date_key='cv_tstamp',
    sort='cv_tstamp',
    dist='cv_id',
    partition_by = snowplow_utils.get_value_by_target_type(bigquery_val = {
      "field": "cv_tstamp",
      "data_type": "timestamp"
    }, databricks_val='cv_tstamp_date'),
    tags=["derived","snowplow_attribution_incremental"],
    sql_header=snowplow_utils.set_query_tag(var('snowplow__query_tag', 'snowplow_dbt')),
    tblproperties={
      'delta.autoOptimize.optimizeWrite' : 'true',
      'delta.autoOptimize.autoCompact' : 'true'
    },
    snowplow_optimize = true
  )
}}
{%- set __, last_processed_cv_tstamp = snowplow_utils.return_limits_from_model(ref('snowplow_subscription_attribution_manifest'),'last_success','last_success',true) %}


 with this_run_subscriptions as (
    select
        distinct subscription_id
    from 
        {{ var('snowplow__conversions_source' )}} as ev
    where 
        {{ var('snowplow__conversion_clause') }} 

        and ev.cv_type in ({{ snowplow_utils.print_list(var('snowplow__subscription_events')) }})
        
        {% if target.type in ['databricks', 'spark'] -%} 
            cv_tstamp_date >= date({{ snowplow_utils.timestamp_add('day', -var("snowplow__lookback_window_hours", 30), last_processed_cv_tstamp) }})
        {% else %} 
            and cv_tstamp >= {{ snowplow_utils.timestamp_add('hour', -var("snowplow__lookback_window_hours", 6), last_processed_cv_tstamp) }}
        {% endif %}
 ), 
 subscription_events AS (
    select
        ev.cv_id,
        ev.event_id,
        ev.subscription_id,

        ev.user_identifier, 
        ev.user_id,
        ev.stitched_user_id,
        
        ev.cv_tstamp,
        cv_type,
        ev.cv_value

    from 
        {{ var('snowplow__conversions_source' )}} as ev
    where 
        {{ var('snowplow__conversion_clause') }} 

        and ev.cv_type in ({{ snowplow_utils.print_list(var('snowplow__subscription_events')) }})
        
        and EXISTS (
                SELECT 1 
                FROM this_run_subscriptions t 
                WHERE t.subscription_id = ev.subscription_id
            )
 )

select * from subscription_events