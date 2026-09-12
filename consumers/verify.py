"""Consumer verifier: row-count checks plus Gold file-sink export (local-only).

Queries Silver/Gold row counts through DuckDB (minimal profile, default) or
Trino (full profile), reports `dbt test` status, and exports the Gold mart to
the file sink::

    s3://<export-bucket>/date=YYYY-MM-DD/export.parquet

Reconcile rule (printed as RECONCILED / MISMATCH / UNKNOWN):
  bronze S3 objects >= 1, silver rows >= 1, 1 <= gold rows <= silver rows.

Exit codes: 0 when counts reconcile and `dbt test` passes (a skipped dbt run
or missing engine data is reported, not fatal); 2 on mismatch when
--fail-on-mismatch is given (argparse usage errors also exit 2).

Usage:
    python consumers/verify.py --engine duckdb --no-export
    python consumers/verify.py --engine trino --trino-host localhost --fail-on-mismatch
    python consumers/verify.py --engine auto --export --export-bucket gold-exports
"""

from __future__ import annotations

import argparse
import datetime as dt
import os
import shutil
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_WAREHOUSE = REPO_ROOT / "warehouse" / "dev.duckdb"
DEFAULT_DBT_DIR = REPO_ROOT / "dbt"
DEFAULT_BRONZE_BUCKET = "bronze"
DEFAULT_EXPORT_BUCKET = "gold-exports"
DEFAULT_TRINO_PORT = 8080

SILVER_TABLE = "events"  # dbt-duckdb dev name for models/silver/events.sql
GOLD_TABLE = "event_marts"  # dbt-duckdb dev name for models/gold/event_marts.sql
TRINO_SILVER = "quickstart_catalog.silver.events"
TRINO_GOLD = "quickstart_catalog.gold.event_marts"
TRINO_BRONZE = "quickstart_catalog.bronze.events"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Verify Bronze/Silver/Gold counts and export Gold to S3."
    )
    parser.add_argument(
        "--engine", choices=("duckdb", "trino", "auto"), default="auto",
        help="Query engine. 'auto' uses Trino when --trino-host is given, "
             "else DuckDB (works without the full profile). Default auto.",
    )
    parser.add_argument(
        "--trino-host", default=None,
        help="Trino coordinator host. Defaults to $TRINO_HOST, then localhost "
             "(only used with --engine trino, or auto with explicit host).",
    )
    parser.add_argument(
        "--trino-port", type=int, default=None,
        help=f"Trino coordinator port. Defaults to $TRINO_PORT, then {DEFAULT_TRINO_PORT}.",
    )
    parser.add_argument(
        "--export", action=argparse.BooleanOptionalAction, default=True,
        help="Export Gold to s3://<export-bucket>/date=<date>/export.parquet "
             "(default on; pass --no-export to skip).",
    )
    parser.add_argument(
        "--fail-on-mismatch", action="store_true",
        help="Exit 2 when counts do not reconcile, dbt tests fail, or the "
             "export fails. Without it, mismatches are reported with exit 0.",
    )
    parser.add_argument(
        "--s3-endpoint", default=None,
        help="S3 endpoint URL. Defaults to S3_ENDPOINT_INTERNAL, then "
             "S3_ENDPOINT_EXTERNAL, then http://localhost:9000.",
    )
    parser.add_argument(
        "--bronze-bucket", default=DEFAULT_BRONZE_BUCKET,
        help=f"Bucket holding producer output. Default {DEFAULT_BRONZE_BUCKET}.",
    )
    parser.add_argument(
        "--export-bucket", default=DEFAULT_EXPORT_BUCKET,
        help=f"File-sink bucket for the Gold export. Default {DEFAULT_EXPORT_BUCKET}.",
    )
    parser.add_argument(
        "--date", default=None,
        help="Export date partition (YYYY-MM-DD). Defaults to today in UTC.",
    )
    parser.add_argument(
        "--warehouse", default=None,
        help=f"DuckDB file for --engine duckdb. Defaults to {DEFAULT_WAREHOUSE}.",
    )
    parser.add_argument(
        "--dbt-dir", default=None,
        help=f"dbt project dir for `dbt test`. Defaults to {DEFAULT_DBT_DIR}.",
    )
    return parser


def resolve_date(value: str | None) -> str:
    if value is None:
        return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d")
    try:
        dt.datetime.strptime(value, "%Y-%m-%d")
    except ValueError:
        raise ValueError(f"--date must be YYYY-MM-DD, got {value!r}")
    return value


def resolve_endpoint(explicit: str | None) -> str:
    return (
        explicit
        or os.environ.get("S3_ENDPOINT_INTERNAL")
        or os.environ.get("S3_ENDPOINT_EXTERNAL")
        or "http://localhost:9000"
    )


def resolve_engine(args: argparse.Namespace) -> tuple[str, str]:
    """Return (engine, note). Auto prefers Trino only with an explicit host."""
    host = args.trino_host or os.environ.get("TRINO_HOST")
    if args.engine == "auto":
        if args.trino_host is not None:
            return "trino", "auto selected trino (explicit --trino-host)"
        return "duckdb", "auto selected duckdb (default, no full profile needed)"
    if args.engine == "trino" and host is None:
        host = "localhost"
    return args.engine, ""


def count_s3_objects(bucket: str, prefix: str, endpoint: str) -> tuple[int | None, str]:
    try:
        import boto3
        from botocore.config import Config
    except ImportError:
        return None, "boto3 not installed (pip install -r consumers/requirements.txt)"
    try:
        client = boto3.client(
            "s3",
            endpoint_url=endpoint,
            aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
            region_name=os.environ.get("AWS_REGION", "us-east-1"),
            config=Config(s3={"addressing_style": "path"}),
        )
        objects = 0
        token: str | None = None
        while True:
            kwargs = {"Bucket": bucket, "Prefix": prefix}
            if token:
                kwargs["ContinuationToken"] = token
            page = client.list_objects_v2(**kwargs)
            objects += page.get("KeyCount", 0)
            if not page.get("IsTruncated"):
                break
            token = page.get("NextContinuationToken")
        return objects, ""
    except Exception as exc:  # noqa: BLE001 — reported, not raised
        return None, f"s3://{bucket}/{prefix} unreachable via {endpoint}: {exc}"


def count_duckdb(warehouse: Path) -> tuple[dict[str, int | None], str]:
    if not warehouse.exists():
        return {"silver": None, "gold": None}, f"warehouse file missing: {warehouse}"
    try:
        import duckdb
    except ImportError:
        return {"silver": None, "gold": None}, \
            "duckdb not installed (pip install -r consumers/requirements.txt)"
    counts: dict[str, int | None] = {}
    notes: list[str] = []
    try:
        con = duckdb.connect(str(warehouse), read_only=True)
    except Exception as exc:  # noqa: BLE001
        return {"silver": None, "gold": None}, f"cannot open {warehouse}: {exc}"
    with con:
        for label, table in (("silver", SILVER_TABLE), ("gold", GOLD_TABLE)):
            try:
                counts[label] = con.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
            except Exception as exc:  # noqa: BLE001, PERF203
                counts[label] = None
                notes.append(f"{table}: {exc}")
    return counts, "; ".join(notes)


def count_trino(host: str, port: int) -> tuple[dict[str, int | None], str]:
    try:
        from trino.dbapi import connect
    except ImportError:
        return {"silver": None, "gold": None}, \
            "trino client not installed (pip install -r consumers/requirements.txt)"
    counts: dict[str, int | None] = {}
    notes: list[str] = []
    try:
        con = connect(host=host, port=port, user="quickstart_user", http_scheme="http")
        cur = con.cursor()
        for label, table in (("silver", TRINO_SILVER), ("gold", TRINO_GOLD)):
            try:
                cur.execute(f"SELECT count(*) FROM {table}")
                counts[label] = cur.fetchone()[0]
            except Exception as exc:  # noqa: BLE001, PERF203
                counts[label] = None
                notes.append(f"{table}: {exc}")
        return counts, "; ".join(notes)
    except Exception as exc:  # noqa: BLE001
        return {"silver": None, "gold": None}, f"trino at {host}:{port} unreachable: {exc}"


def run_dbt_test(dbt_dir: Path) -> tuple[str, str]:
    """Return (status, detail) with status in passed/failed/skipped."""
    if shutil.which("dbt") is None:
        return "skipped", "dbt binary not on PATH (run inside dbt-runner, see runbook)"
    try:
        proc = subprocess.run(
            ["dbt", "test", "--profiles-dir", str(dbt_dir)],
            cwd=str(dbt_dir), capture_output=True, text=True, timeout=600,
        )
    except subprocess.TimeoutExpired:
        return "failed", "dbt test timed out after 600s"
    tail = (proc.stdout + proc.stderr).strip().splitlines()[-5:]
    detail = " | ".join(tail) if tail else f"exit {proc.returncode}"
    return ("passed", detail) if proc.returncode == 0 else ("failed", detail)


def fetch_gold_duckdb(warehouse: Path) -> tuple[list[str] | None, list[tuple], str]:
    try:
        import duckdb
    except ImportError:
        return None, [], "duckdb not installed (pip install -r consumers/requirements.txt)"
    if not warehouse.exists():
        return None, [], f"warehouse file missing: {warehouse}"
    try:
        con = duckdb.connect(str(warehouse), read_only=True)
        with con:
            cur = con.execute(f"SELECT * FROM {GOLD_TABLE}")
            columns = [d[0] for d in cur.description]
            return columns, cur.fetchall(), ""
    except Exception as exc:  # noqa: BLE001
        return None, [], f"cannot read {GOLD_TABLE}: {exc}"


def export_gold_parquet(columns: list[str], rows: list[tuple],
                        bucket: str, day: str, endpoint: str) -> str:
    try:
        import boto3
        from botocore.config import Config
    except ImportError:
        raise RuntimeError("boto3 is required for --export: "
                           "pip install -r consumers/requirements.txt")
    try:
        import io

        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError:
        raise RuntimeError("pyarrow is required for --export: "
                           "pip install -r consumers/requirements.txt")
    table = pa.Table.from_pylist([dict(zip(columns, r)) for r in rows])
    buf = io.BytesIO()
    pq.write_table(table, buf)
    key = f"date={day}/export.parquet"
    client = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
        region_name=os.environ.get("AWS_REGION", "us-east-1"),
        config=Config(s3={"addressing_style": "path"}),
    )
    client.put_object(Bucket=bucket, Key=key, Body=buf.getvalue())
    return f"s3://{bucket}/{key}"


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        day = resolve_date(args.date)
    except ValueError as exc:
        parser.error(str(exc))
    engine, engine_note = resolve_engine(args)
    endpoint = resolve_endpoint(args.s3_endpoint)
    warehouse = Path(args.warehouse) if args.warehouse else DEFAULT_WAREHOUSE
    dbt_dir = Path(args.dbt_dir) if args.dbt_dir else DEFAULT_DBT_DIR
    port = args.trino_port or int(os.environ.get("TRINO_PORT", DEFAULT_TRINO_PORT))

    problems: list[str] = []

    bronze_objects, bronze_note = count_s3_objects(args.bronze_bucket, "events/", endpoint)
    if engine == "trino":
        host = args.trino_host or os.environ.get("TRINO_HOST", "localhost")
        counts, engine_detail = count_trino(host, port)
        print(f"engine: trino ({host}:{port})")
    else:
        counts, engine_detail = count_duckdb(warehouse)
        print(f"engine: duckdb ({warehouse})")
    if engine_note:
        print(f"note: {engine_note}")

    print(f"bronze: s3://{args.bronze_bucket}/events/ objects={bronze_objects}"
          + (f" ({bronze_note})" if bronze_note else ""))
    print(f"silver: {counts['silver']} rows"
          + (f" ({engine_detail})" if counts["silver"] is None and engine_detail else ""))
    print(f"gold: {counts['gold']} rows"
          + (f" ({engine_detail})" if counts["gold"] is None and engine_detail else ""))

    silver, gold = counts["silver"], counts["gold"]
    if bronze_objects is None or silver is None or gold is None:
        verdict = "UNKNOWN"
        problems.append("incomplete counts (engine or S3 unreachable)")
    elif bronze_objects >= 1 and silver >= 1 and 1 <= gold <= silver:
        verdict = "RECONCILED"
    else:
        verdict = "MISMATCH"
        problems.append(
            f"counts out of band: bronze_objects={bronze_objects} silver={silver} gold={gold} "
            "(expect bronze>=1, silver>=1, 1<=gold<=silver)")

    dbt_status, dbt_detail = run_dbt_test(dbt_dir)
    print(f"dbt test: {dbt_status} ({dbt_detail})")
    if dbt_status == "failed":
        problems.append("dbt test failed")

    export_path = "skipped (--no-export)"
    if args.export:
        if engine != "duckdb":
            export_path = "skipped (export reads the local DuckDB gold table; rerun with --engine duckdb)"
        else:
            columns, rows, fetch_note = fetch_gold_duckdb(warehouse)
            if columns is None:
                export_path = f"failed ({fetch_note})"
                problems.append(f"export failed: {fetch_note}")
            elif not rows:
                export_path = "failed (gold table is empty)"
                problems.append("export failed: gold table is empty")
            else:
                try:
                    export_path = export_gold_parquet(columns, rows, args.export_bucket, day, endpoint)
                except Exception as exc:  # noqa: BLE001
                    export_path = f"failed ({exc})"
                    problems.append(f"export failed: {exc}")
    print(f"export: {export_path}")

    print(f"verdict: {verdict}")
    if problems:
        for problem in problems:
            print(f"  - {problem}")
    if args.fail_on_mismatch and (verdict != "RECONCILED" or problems):
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
