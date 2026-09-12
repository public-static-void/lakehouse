{{ config(materialized='table') }}

/*
 * Silver events: typed, cleaned, deduplicated (SPEC R007, PLAN P009).
 * The producer injects same-event_id duplicates with a bumped load_time
 * (--dup-rate); the window below keeps the latest ingested copy per
 * event_id. Late arrivals (--late-rate backdates event_time) are kept —
 * event_time is history, load_time orders ingestion.
 */
with typed as (
    select
        event_id,
        event_time,
        upper(trim(source_system)) as source_system,
        load_time,
        batch_id,
        lower(trim(op)) as op,
        user_id,
        lower(trim(event_type)) as event_type,
        amount,
        upper(trim(currency)) as currency,
        upper(trim(country)) as country
    from {{ ref('stg_events') }}
    where event_id is not null
      and op in ('c', 'u', 'd')
),

deduped as (
    select
        *,
        row_number() over (
            partition by event_id
            order by load_time desc
        ) as rn
    from typed
)

select
    event_id,
    event_time,
    source_system,
    load_time,
    batch_id,
    op,
    user_id,
    event_type,
    amount,
    currency,
    country
from deduped
where rn = 1
