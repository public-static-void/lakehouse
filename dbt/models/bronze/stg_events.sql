{{ config(materialized='view') }}

/*
 * Bronze staging view over the raw producer output (SPEC R007).
 * Applies explicit typing only — no filtering or dedup here, so Silver
 * sees every ingested row (including duplicates/late arrivals injected
 * via --dup-rate/--late-rate) and downstream tests stay meaningful.
 */
select
    cast(event_id as varchar) as event_id,
    cast(event_time as timestamp) as event_time,
    cast(source_system as varchar) as source_system,
    cast(load_time as timestamp) as load_time,
    cast(batch_id as varchar) as batch_id,
    cast(op as varchar) as op,
    cast(user_id as varchar) as user_id,
    cast(event_type as varchar) as event_type,
    cast(amount as double) as amount,
    cast(currency as varchar) as currency,
    cast(country as varchar) as country
from {{ source('bronze', 'events') }}
