/*
 * Seed-source consistency guard: the bronze.events source must resolve to
 * the relation materialized by `dbt seed` (the sample seed aliased into the
 * bronze schema). Singular tests fail when rows are returned, so this query
 * fails the gate when the source is empty — the observable symptom of the
 * seed landing in a different schema or under a different name — and errors
 * when the source relation is missing entirely. Renaming either side (seed
 * schema/alias or source name/table) therefore breaks `dbt test`/`dbt build`
 * instead of surfacing later as a missing-schema error in stg_events.
 */
select 1 as bronze_source_empty
where not exists (
    select 1 from {{ source('bronze', 'events') }}
)
