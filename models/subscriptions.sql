{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    on_schema_change='append_new_columns',
    unique_key='subscription_id',
    upsert_date_key='last_subscription_event_tstamp',
    sort='subscription_start_tstamp',
    dist='cv_id',
    partition_by = snowplow_utils.get_value_by_target_type(bigquery_val = {
      "field": "subscription_start_tstamp",
      "data_type": "timestamp"
    }, databricks_val='cv_tstamp_date'),
    tags=["derived"],
    sql_header=snowplow_utils.set_query_tag(var('snowplow__query_tag', 'snowplow_dbt')),
    tblproperties={
      'delta.autoOptimize.optimizeWrite' : 'true',
      'delta.autoOptimize.autoCompact' : 'true'
    },
    snowplow_optimize = true
  )
}}

with first_subscription_event AS (
  select
    ev.subscription_id,
    ev.user_identifier,
    ev.user_id,
    ev.stitched_user_id,
    ev.cv_tstamp,
    ev.cv_type,
    ev.cv_value
  from {{ ref('subscription_events_this_run' )}} as ev
  where
    
  qualify row_number() over (partition by ev.subscription_id order by ev.cv_tstamp) = 1
),
 subscription_events_aggregated AS (
select
    ev.subscription_id,

    max(ev.cv_tstamp) as last_subscription_event_tstamp,

    {% for event in var('snowplow__subscription_events') %}
    count(distinct case when ev.cv_type = '{{ event }}' then ev.cv_id end) as {{ event }}_count,
    {% endfor %}

    {% for event in var('snowplow__subscription_events') %}
    sum(case when ev.cv_type = '{{ event }}' then ev.cv_value else 0 end) as {{ event }}_revenue,
    {% endfor %}

    sum(ev.cv_value) as revenue

from {{ ref('subscription_events_this_run' )}} as ev

{{ dbt_utils.group_by(n=1) }}
)

select
  first_subscription_event.subscription_id,
  first_subscription_event.user_identifier,
  first_subscription_event.user_id,
  first_subscription_event.stitched_user_id,
  first_subscription_event.cv_tstamp as subscription_start_tstamp,
  subscription_events_aggregated.last_subscription_event_tstamp as last_subscription_event_tstamp,
  subscription_events_aggregated.revenue as revenue,

  {% for event in var('snowplow__subscription_events') %}
  subscription_events_aggregated.{{ event }}_count,
  {% endfor %}

  {% for event in var('snowplow__subscription_events') %}
  subscription_events_aggregated.{{ event }}_revenue,
  {% endfor %}

from 
  first_subscription_event
left join 
  subscription_events_aggregated
on 
  first_subscription_event.subscription_id = subscription_events_aggregated.subscription_id
