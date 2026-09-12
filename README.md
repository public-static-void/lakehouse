# Local Lakehouse (Podman-first, FOSS, no cloud)

Small-scale local showcase of `docs/lakehouse.md`: Bronze → Silver → Gold on
S3-compatible storage (RustFS) + Iceberg REST catalog (Polaris), transformed
with dbt, queried with DuckDB (default) or Trino (full profile), with a
synthetic producer/consumer simulating real sources/sinks.

Two profiles: `minimal` (default, ~3 GB) and `full` (adds Trino, Superset,
Airflow — needs ≥8 GB RAM). Stay on `minimal` on small laptops.

## Prerequisites

- Podman ≥4 with the Podman socket enabled, **or** Docker
- `openssl` (secret generation), `python3` (producer/consumer/dbt)
- 6 GB free RAM for `minimal`, 8 GB+ for `full`
- Free ports: 9000–9001, 8080–8081, 8088, 8181–8182 (see `docs/runbook.md`)

## Quickstart (minimal)

```sh
cp .env.example .env            # then replace every CHANGE_ME (openssl rand -hex 32)
./scripts/preflight.sh          # checks Podman, compose provider, RAM/CPU, ports
export DOCKER_HOST="unix://$XDG_RUNTIME_DIR/podman/podman.sock"   # primary runner
docker compose pull             # minimal profile pulls only RustFS + Polaris + Python
docker compose up -d            # minimal stack (default, no --profile flag)
python producers/events.py --sink s3 --batch-size 500 --max-batches 20 --seed 42
cd dbt && dbt build --profiles-dir . && dbt test --profiles-dir . && cd ..
python consumers/verify.py --engine duckdb        # counts + dbt status + Gold export
docker compose down             # stop; add -v / volume rm below for full reset
```

Full profile:

```sh
docker compose --profile full up -d
python consumers/verify.py --engine trino --fail-on-mismatch
# Superset: http://localhost:8088 (admin/admin) — add a Trino dataset after login
```

Fallback runner (`podman-compose` ignores health-gated `depends_on`; the
bucket/bootstrap retry loops self-heal, just re-run on first failure):

```sh
podman-compose up -d
podman-compose --profile full up -d
```

Cleanup (deletes all lake data):

```sh
docker compose down -v
podman volume rm lakehouse_rustfs-data lakehouse_polaris-data lakehouse_warehouse-data
```

## Layout

| Path | What |
|---|---|
| `compose.yaml` | `minimal` default + `full` (+ commented `extensions` stubs) |
| `producers/events.py` | Synthetic Bronze source (`--sink stdout` previews without S3) |
| `dbt/` | Bronze → Silver → Gold models + contract tests |
| `consumers/verify.py` | Row-count checks + `dbt test` status + Gold → `s3://gold-exports/` |
| `trino/catalog/polaris.properties` | Trino ↔ Polaris REST wiring (full profile) |
| `scripts/preflight.sh`, `scripts/bootstrap-polaris.sh` | Host gates + idempotent catalog bootstrap |
| `docs/runbook.md` | Full runbook: runners, queries, troubleshooting, extensions |

Details, sample queries, and failure remedies: **`docs/runbook.md`**.
