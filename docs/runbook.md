# Runbook — local lakehouse (gen-0)

Minimal-first guide: every section states which profile it needs. `minimal`
is the default (`docker compose up` with no flags); `full` is opt-in
(`--profile full`) and needs ≥8 GB RAM.

## 1. Prerequisites

- Podman ≥4 **or** Docker; `openssl`; `python3` + `pip`
- Python deps per component: `pip install -r producers/requirements.txt`,
  `pip install -r consumers/requirements.txt`, `pip install -r dbt/requirements.txt`
- RAM: ≥6 GB free for `minimal` (warns below), ≥8 GB for `full`
- Ports free: 9000 (S3), 9001 (console), 8181 (Polaris API), 8182 (health),
  8080 (Trino, full), 8088 (Superset, full), 8081 (Airflow, full)

## 2. Compose runners (contract)

**Primary — `docker compose` against the Podman socket:**

```sh
systemctl --user enable --now podman.socket   # one-time: exposes the socket
export DOCKER_HOST="unix://$XDG_RUNTIME_DIR/podman/podman.sock"
docker compose up -d                          # minimal
docker compose --profile full up -d           # full
```

**Fallback — `podman-compose`:**

```sh
podman-compose up -d
podman-compose --profile full up -d
```

Caveat (E01): `podman-compose` ignores `depends_on: condition:
service_healthy/completed_successfully`, so `bucket-setup` and
`polaris-setup` may start before RustFS/Polaris are ready. Both scripts retry
with backoff and are idempotent (re-runs exit 0, 409 = already-exists), so a
second `up` converges. Prefer the socket runner for a clean first boot.

## 3. Configure

```sh
cp .env.example .env
# Replace every CHANGE_ME value; generate with: openssl rand -hex 32
# (AIRFLOW_FERNET_KEY needs a Fernet key — see .env.example comment.)
# Endpoints stay as-is unless ports are remapped:
#   S3_ENDPOINT_INTERNAL=http://rustfs:9000  (in-container, service DNS)
#   S3_ENDPOINT_EXTERNAL=http://localhost:9000 (host tooling)
```

## 4. Preflight, pull, up/down

```sh
./scripts/preflight.sh        # exit 0 pass / 1 fail with >>> FIX: lines / 2 warn-only
docker compose pull           # explicit pull step: multi-GB first pull on slow links
docker compose up -d          # minimal (default)
docker compose --profile full up -d   # full (opt-in)
docker compose ps             # rustfs/polaris healthy; polaris-setup completed
docker compose down           # stop, keep volumes (data survives)
docker compose down -v        # stop + delete named volumes (full reset)
podman volume rm lakehouse_rustfs-data lakehouse_polaris-data \
  lakehouse_warehouse-data lakehouse_trino-data \
  lakehouse_superset-meta lakehouse_airflow-meta   # per-volume cleanup
```

If `polaris-setup` exits non-zero on the fallback runner, just re-run `up`
(E01 self-heal); success prints `BOOTSTRAP OK`.

## 5. Seed (synthetic source)

```sh
# Preview without S3 (no dependencies needed):
python producers/events.py --sink stdout --batch-size 5 --max-batches 2 --seed 7
# Bronze to S3 (default: 500 x 20 = 10_000 rows; hard caps: rate<=100, batch<=5000):
python producers/events.py --sink s3 --rate 10 --batch-size 500 --max-batches 20 --seed 42
# Continuous mode needs an explicit bound:
python producers/events.py --sink s3 --loop --loop-max-batches 10 --seed 1
```

Determinism: same `--seed` replays the same `event_id`s, duplicates
(`--dup-rate`, same id + bumped `load_time`, absorbed by the Silver dedup
window) and late rows (`--late-rate`, backdated `event_time` 2–7 days).

## 6. Transform (dbt Bronze → Silver → Gold)

```sh
cd dbt && dbt build --profiles-dir . && dbt test --profiles-dir . && cd ..
# Inside the stack instead (same layout via the warehouse-data mount):
docker compose exec dbt-runner sh -c 'cd /work/dbt && dbt build --profiles-dir .'
```

`stg_events` (Bronze view) → `events` (Silver: typed, `op` filtered,
`row_number() over (partition by event_id order by load_time desc)`
dedup) → `event_marts` (Gold: one row per day × type × country with
`event_count`, `distinct_users`, `revenue`, `refunds`). `schema.yml`
enforces `event_id` unique + `load_time`/`source_system`/`batch_id` not-null.
Full profile: `dbt build --profiles-dir . --target prod` runs the same models
through Trino (`query_max_memory=5GB`); dbt Fusion `catalogs.yml` v2 is
explicitly out of scope for gen-0.

## 7. Sample queries

DuckDB — default engine, **no `full` profile needed** (uses
`warehouse/dev.duckdb` from the dbt `dev` target):

```sql
SELECT count(*) FROM events;                       -- Silver rows
SELECT event_date, event_type, country, event_count, revenue
  FROM event_marts ORDER BY event_date DESC LIMIT 20;
SELECT event_type, count(*), sum(amount)
  FROM events GROUP BY 1 ORDER BY 2 DESC;
```

Trino — full profile only (`http://localhost:8080`, catalog
`quickstart_catalog`, realm `POLARIS`):

```sql
SELECT count(*) FROM quickstart_catalog.silver.events;
SELECT event_date, event_type, country, event_count, revenue
  FROM quickstart_catalog.gold.event_marts ORDER BY event_date DESC LIMIT 20;
```

## 8. Verify + file sink (consumer)

```sh
python consumers/verify.py --engine duckdb --no-export   # offline-friendly check
python consumers/verify.py --engine duckdb               # + Gold export
python consumers/verify.py --engine trino --fail-on-mismatch   # full profile gate
```

Prints Bronze S3 objects / Silver rows / Gold rows, `dbt test` status, and the
export path `s3://gold-exports/date=YYYY-MM-DD/export.parquet` (or
`--export-bucket` override). Verdicts: `RECONCILED` (bronze ≥1 object,
silver ≥1, 1 ≤ gold ≤ silver), `MISMATCH`, or `UNKNOWN` (engine/S3
unreachable — reported, exit 0 unless `--fail-on-mismatch`, which exits 2).

## 9. Superset (full profile)

1. `docker compose --profile full up -d`, wait for `superset` healthy.
2. Open `http://localhost:8088`, log in with `admin` / `admin`
   (created idempotently by the service entrypoint; change it for shared hosts).
3. Add database: Trino + `trino://quickstart_user@trino:8080/quickstart_catalog`
   (Superset reaches Trino over compose DNS, not `localhost`).
4. Add a dataset on `gold.event_marts` and chart `revenue` by `country`.

## 10. Troubleshooting

| Symptom | Fix |
|---|---|
| Preflight names a busy port (E07) | Stop the conflicting process or remap the published port, then re-run preflight |
| `podman-compose` boots out of order (E01) | Re-run `up`; retry loops + idempotent setup converge |
| S3 403 / signature errors (E03) | Keep path-style + dual endpoints: containers use `http://rustfs:9000`, host uses `http://localhost:9000`; never mix them |
| Rootless volume permission denied, RustFS UID (E02) | State lives in named volumes (not bind-mounts) by design; for host bind-mount experiments add `:Z` and match the container UID |
| Polaris bootstrap 409 on re-run (E04) | Normal: create-or-skip treats it as success, `BOOTSTRAP OK`, exit 0 |
| OOM on 8 GB laptop (E06) | Stay on `minimal` (default); `full` (Trino `-Xmx4G`, Airflow/Superset caps) needs ≥8 GB free |
| Slow first start (E10) | `docker compose pull` first; minimal pulls only RustFS + Polaris + Python |

## 11. Extension stubs

`compose.yaml` ends with commented `profiles: [extensions]` blocks — one
paragraph each, ports/volumes reserved, nothing starts: Kafka/Debezium (9092,
8083), Spark/Flink (8082, 4040, 8084), NiFi (8443), ClickHouse (8123, 9002),
Airbyte/Meltano (8000), OpenMetadata/Marquez (8585, 5000),
Keycloak/OPA/OpenBao (8090, 8183, 8200), Prometheus/Grafana/Loki (9090, 3000,
3100). Uncomment a block and run with `--profile extensions`.
