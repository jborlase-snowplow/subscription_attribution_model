{% macro default__attribution_overview() %}

{%- set __, last_processed_cv_tstamp = snowplow_utils.return_limits_from_model(ref('snowplow_attribution_campaign_attributions'),'cv_tstamp','cv_tstamp',true) %}

-- making sure we only include spend that is needed
with spend_with_unique_keys as (
  {% if var('snowplow__spend_source') != 'not defined' %}
    select row_number() over(order by spend_tstamp) as spend_id, *
    from {{ var('snowplow__spend_source') }}
  {% else %}
    select true
  {% endif %}
)

-- we need to dedupe as the join does the filtering, we can't group them upfront
, campaign_spend as (

  {% if var('snowplow__spend_source') != 'not defined' %}
    select s.campaign, s.spend, row_number() over (partition by s.spend_id order by s.spend_tstamp) as row_num
    from spend_with_unique_keys s
    inner join {{ ref('snowplow_attribution_campaign_attributions') }} c
    on c.campaign = s.campaign and s.spend_tstamp < cv_tstamp 
    and s.spend_tstamp > {{ snowplow_utils.timestamp_add('day', -90, 'cv_tstamp') }}
    where s.campaign is not null  
  {% else %}
    select true
  {% endif %}
    
)

, channel_spend as (
  
  {% if var('snowplow__spend_source') != 'not defined' %}
    select s.channel, s.spend, row_number() over (partition by s.spend_id order by s.spend_tstamp) as row_num
    from spend_with_unique_keys s
    inner join {{ ref('snowplow_attribution_channel_attributions') }} c
    on c.channel = s.channel and s.spend_tstamp < cv_tstamp
    and s.spend_tstamp > {{ snowplow_utils.timestamp_add('day', -90, 'cv_tstamp') }}
    where s.channel is not null  
  {% else %}
    select true
  {% endif %}
    
)

-- grouping spend to avoid duplicates in later join
, campaign_spend_grouped as (

  {% if var('snowplow__spend_source') != 'not defined' %}
    select campaign, sum(spend) as spend
    from campaign_spend
    where row_num = 1
    group by 1
  
  {% else %}
    select true
  {% endif %}
    
)

, channel_spend_grouped as (
  
  {% if var('snowplow__spend_source') != 'not defined' %}
    select channel, sum(spend) as spend
    from channel_spend
    where row_num = 1
    group by 1
  
  {% else %}
    select true
  {% endif %}
    
)


, campaign_prep as (
  
  select
    c.cv_id,
    c.event_id,
    c.campaign,
    c.cv_tstamp,
    c.cv_type,
    sum(c.first_touch_attribution) as first_touch_attribution,
    sum(c.last_touch_attribution) as last_touch_attribution,
    sum(c.linear_attribution) as linear_attribution,
    sum(c.position_based_attribution) as position_based_attribution,
    coalesce(min(c.cv_total_revenue),0) as cv_total_revenue,

    sum(c.first_touch_attribution*cv_total_revenue) as first_touch_attribution_revenue, -- not too sure I follow how the calculation is meant to be, in case a conversion has multiple path elements we multiply each element with the total conversion value here, the total values in the output tables also don't add up (related to this probably?), the total cv_value in the conversions table is 149 yet the values are much higher in the overview 
    sum(c.last_touch_attribution*cv_total_revenue) as last_touch_attribution_revenue, -- most likely you would need to take the join in a separate cte and correct it once it is already aggregated or something similar
    sum(c.linear_attribution*cv_total_revenue) as linear_attribution_revenue,
    sum(c.position_based_attribution*cv_total_revenue) as position_based_attribution_revenue,

    {% for event in var('snowplow__subscription_events') %}
     sum({{event}}_count) * sum(c.first_touch_attribution) as {{ event }}_first_touch_attribution,
     sum({{event}}_count) * sum(c.last_touch_attribution) as {{ event }}_last_touch_attribution,
     sum({{event}}_count) * sum(c.linear_attribution) as {{ event }}_linear_attribution,
     sum({{event}}_count) * sum(c.position_based_attribution) as {{ event }}_position_based_attribution,

     sum({{event}}_revenue) * sum(c.first_touch_attribution) as {{ event }}_first_touch_attribution_revenue,
     sum({{event}}_revenue) * sum(c.last_touch_attribution) as {{ event }}_last_touch_attribution_revenue,
     sum({{event}}_revenue) * sum(c.linear_attribution) as {{ event }}_linear_attribution_revenue,
     sum({{event}}_revenue) * sum(c.position_based_attribution) as {{ event }}_position_based_attribution_revenue, --free trial attributed revenue does not currently have a test case (in the output, most likely fine just noticed), also it would be great to add a non subscription related test case too just to see if the totals add up for that logic too (not sure how much we deviated on that front, you understand it better so might not be needed)
    {% endfor %}    
    
    {% if var('snowplow__spend_source') != 'not defined' %}
      min(s.spend) as spend
    {% else %}
      cast(null as {{ dbt.type_numeric() }}) as spend
    {% endif %}
    
  from {{ ref('snowplow_attribution_campaign_attributions') }} c
  
  {% if var('snowplow__spend_source') != 'not defined' %}
    left join campaign_spend_grouped s --be careful about giving the same alias, it has the potential to break things later on
    on s.campaign = c.campaign
  {% endif %}

  left join {{ ref('subscriptions') }} s
    on c.cv_id = s.subscription_id
  
  where
  {% if not var('snowplow__conversion_window_start_date') == '' and not var('snowplow__conversion_window_end_date') == '' %}
    cv_tstamp >= '{{ var("snowplow__conversion_window_start_date") }}' and cv_tstamp < '{{ var("snowplow__conversion_window_end_date") }}'
  {% else %}
    cv_tstamp >= {{ snowplow_utils.timestamp_add('day', -var("snowplow__conversion_window_days"), last_processed_cv_tstamp) }}
  {% endif%}
  
  {{ dbt_utils.group_by(n=5) }}
)

, channel_prep as (
  
  select
    c.cv_id,
    c.event_id,
    c.cv_tstamp,
    c.cv_type,
    c.channel,
    sum(c.first_touch_attribution) as first_touch_attribution,
    sum(c.last_touch_attribution) as last_touch_attribution,
    sum(c.linear_attribution) as linear_attribution,
    sum(c.position_based_attribution) as position_based_attribution,
    coalesce(min(c.cv_total_revenue),0) as cv_total_revenue,

    sum(c.first_touch_attribution*cv_total_revenue) as first_touch_attribution_revenue,
    sum(c.last_touch_attribution*cv_total_revenue) as last_touch_attribution_revenue,
    sum(c.linear_attribution*cv_total_revenue) as linear_attribution_revenue,
    sum(c.position_based_attribution*cv_total_revenue) as position_based_attribution_revenue,

    {% for event in var('snowplow__subscription_events') %}
     sum({{event}}_count) * sum(c.first_touch_attribution) as {{ event }}_first_touch_attribution,
     sum({{event}}_count) * sum(c.last_touch_attribution) as {{ event }}_last_touch_attribution,
     sum({{event}}_count) * sum(c.linear_attribution) as {{ event }}_linear_attribution,
     sum({{event}}_count) * sum(c.position_based_attribution) as {{ event }}_position_based_attribution,

     sum({{event}}_revenue) * sum(c.first_touch_attribution) as {{ event }}_first_touch_attribution_revenue,
     sum({{event}}_revenue) * sum(c.last_touch_attribution) as {{ event }}_last_touch_attribution_revenue,
     sum({{event}}_revenue) * sum(c.linear_attribution) as {{ event }}_linear_attribution_revenue,
     sum({{event}}_revenue) * sum(c.position_based_attribution) as {{ event }}_position_based_attribution_revenue,
    {% endfor %}    
  
  {% if var('snowplow__spend_source') != 'not defined' %}
    min(s.spend) as spend
  {% else %}
    cast(null as {{ dbt.type_numeric() }}) as spend
  {% endif %}
  
  from {{ ref('snowplow_attribution_channel_attributions') }} c
  
  {% if var('snowplow__spend_source') != 'not defined' %}
    left join channel_spend_grouped s
    on s.channel = c.channel
  {% endif %}

  left join {{ ref('subscriptions') }} s
    on c.cv_id = s.subscription_id
  
  where 
  
  {% if not var("snowplow__conversion_window_start_date") == '' and not var("snowplow__conversion_window_end_date") == '' %}
    cv_tstamp >= '{{ var("snowplow__conversion_window_start_date") }}' and cv_tstamp < '{{ var("snowplow__conversion_window_end_date") }}'
  {% else %}
    cv_tstamp >= {{ snowplow_utils.timestamp_add('day', -var("snowplow__conversion_window_days"), last_processed_cv_tstamp) }}
  {% endif %}
  
  {{ dbt_utils.group_by(n=5) }}
)

, unions as (
  
  {% set attribution_list = var('snowplow__attribution_list') %}
  
  {% for attribution in attribution_list %}
    select
      'channel' as path_type,
      '{{ attribution }}' as attribution_type,
      channel as touch_point,
      count(distinct cv_id) as in_n_conversion_paths,
      sum({{ attribution }}_attribution) as attributed_conversions,
      min(cv_tstamp) as min_cv_tstamp,
      max(cv_tstamp) as max_cv_tstamp,
      min(spend) as spend,
      sum(cv_total_revenue) as sum_cv_total_revenue,
      sum({{ attribution }}_attribution_revenue) as attributed_revenue,

      {% for event in var('snowplow__subscription_events') %}
        sum({{ event }}_{{ attribution }}_attribution) as {{ event }}_attributed_conversions,
      {% endfor %}

      {% for event in var('snowplow__subscription_events') %}
        sum({{ event }}_{{ attribution }}_attribution_revenue) as {{ event }}_attributed_revenue,
      {% endfor %}
      
    from channel_prep
    
    {{ dbt_utils.group_by(n=3) }}
    
    union all
  {% endfor %}
   
  {% for attribution in attribution_list %}
    select
      'campaign' as path_type,
      '{{ attribution }}' as attribution_type,
      campaign as touch_point,
      count(distinct cv_id) as in_n_conversion_paths,
      sum({{ attribution }}_attribution) as attributed_conversions,
      min(cv_tstamp) as min_cv_tstamp,
      max(cv_tstamp) as max_cv_tstamp,
      min(spend) as spend,
      sum(cv_total_revenue) as sum_cv_total_revenue,
      sum({{ attribution }}_attribution_revenue) as attributed_revenue,

      {% for event in var('snowplow__subscription_events') %}
        sum({{ event }}_{{ attribution }}_attribution) as {{ event }}_attributed_conversions,
      {% endfor %}

      {% for event in var('snowplow__subscription_events') %}
        sum({{ event }}_{{ attribution }}_attribution_revenue) as {{ event }}_attributed_revenue,
      {% endfor %}
      
    from campaign_prep
    
    {{ dbt_utils.group_by(n=3) }}
    
    {%- if not loop.last %}
      union all
    {% endif %}
  {% endfor %}
  
)

select
  path_type,
  attribution_type,
  touch_point,
  in_n_conversion_paths,
  attributed_conversions,
  min_cv_tstamp,
  max_cv_tstamp,
  spend,
  sum_cv_total_revenue,
  attributed_revenue,

  {% for event in var('snowplow__subscription_events') %}
      {{ event }}_attributed_conversions,
    {% endfor %}

    {% for event in var('snowplow__subscription_events') %}
      {{ event }}_attributed_revenue,
    {% endfor %}

  {% if var('snowplow__spend_source') != 'not defined' %}
    , sum(attributed_revenue) / nullif(sum(spend), 0) as ROAS
  {% endif %}

from unions

where touch_point is not null

group by all
 --{{ dbt_utils.group_by(n=10) }}

order by 1,2,3
  
{% endmacro %}
