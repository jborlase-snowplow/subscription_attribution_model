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

    {% if target.type in ['postgres','spark'] %}
          , row_number() over (partition by ev.subscription_id order by ev.cv_tstamp) as session_dedupe_index
    {% endif %}

  from {{ ref('subscription_events_this_run' )}} as ev
  {% if target.type not in ['postgres','spark'] %}
    qualify row_number() over (partition by ev.subscription_id order by ev.cv_tstamp) = 1
  {% endif %}
),
 subscription_events_aggregated AS (
select
    ev.subscription_id,

    max(ev.cv_tstamp) as last_subscription_event_tstamp

    {% for event in var('snowplow__subscription_events') %}
      ,count(distinct case when ev.cv_type = '{{ event }}' then ev.cv_id end) as {{ event }}_count
    {% endfor %}

    {% for event in var('snowplow__subscription_events') %}
      ,sum(case when ev.cv_type = '{{ event }}' then ev.cv_value else 0 end) as {{ event }}_revenue
    {% endfor %}

    ,sum(ev.cv_value) as revenue

from {{ ref('subscription_events_this_run' )}} as ev

{{ dbt_utils.group_by(n=1) }}
)

select
  f.subscription_id,
  f.user_identifier,
  f.user_id,
  f.stitched_user_id,
  f.cv_tstamp as subscription_start_tstamp,
  agg.last_subscription_event_tstamp as last_subscription_event_tstamp,
  agg.revenue as revenue

  {% for event in var('snowplow__subscription_events') %}
    ,agg.{{ event }}_count
  {% endfor %}

  {% for event in var('snowplow__subscription_events') %}
    ,agg.{{ event }}_revenue
  {% endfor %}

from 
  first_subscription_event as f
left join 
  subscription_events_aggregated as agg
on 
  f.subscription_id = agg.subscription_id
{% if target.type in ['postgres','spark'] %}
  where f.session_dedupe_index = 1
{%- endif %}
