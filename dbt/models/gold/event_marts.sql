{{ config(materialized='table') }}

/*
 * Gold mart: daily counts and revenue by event_type/country (PLAN P010).
 * Reads the deduplicated Silver table; one row per (day, type, country).
 * Exported to s3://gold-exports/ by consumers/verify.py (SPEC R008).
 */
select
    cast(event_time as date) as event_date,
    event_type,
    country,
    count(*) as event_count,
    count(distinct user_id) as distinct_users,
    sum(case when event_type = 'purchase' then amount else 0.0 end) as revenue,
    sum(case when event_type = 'refund' then amount else 0.0 end) as refunds
from {{ ref('events') }}
group by 1, 2, 3
