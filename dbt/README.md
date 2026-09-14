# dbt: Bronze → Silver → Gold (M5)

Stable gen-0 path is **dbt-duckdb** (SPEC R007). No live stack needed for
the local `dev` target — seeds provide sample Bronze rows.

```sh
pip install -r requirements.txt
cd dbt
dbt seed --profiles-dir .     # loads seeds/bronze_events_sample.csv
dbt build --profiles-dir .    # staging view -> Silver dedup -> Gold marts
dbt test --profiles-dir .     # event_id unique + not-null contract tests
```

Against the live stack (minimal profile, `dbt-runner` service mounts
`./dbt` at `/work/dbt`):

```sh
docker compose exec dbt-runner sh -c 'pip install -q -r dbt/requirements.txt && cd /work/dbt && dbt seed --profiles-dir . && dbt build --profiles-dir .'
```

`dbt seed` is mandatory before `dbt build`: it materializes
`dev.bronze.events` (seed `bronze_events_sample` aliased into schema `bronze`),
the relation `stg_events` reads via `{{ source('bronze', 'events') }}`.

Full profile: `dbt build --profiles-dir . --target prod` runs the same
models through Trino (`quickstart_catalog`, schema `gold`).

Models: `models/bronze/stg_events.sql` (typed view over the `bronze`
Iceberg namespace) → `models/silver/events.sql` (dedup window keeps the
latest `load_time` per `event_id`) → `models/gold/event_marts.sql`
(daily counts/revenue by `event_type`/`country`). Tests: `models/schema.yml`.

Out of scope for gen-0: dbt Fusion `catalogs.yml` v2 (SPEC dbt contract).
