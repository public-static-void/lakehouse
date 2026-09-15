"""Small-scale synthetic Bronze event producer (local-only).

Generates deterministic fake events and writes them either to stdout
(one JSON object per line) or to S3 in the Bronze layout::

    s3://<bronze-bucket>/events/dt=YYYY-MM-DD/batch=<batch_id>/part-<n>.{json,parquet}

Small-scale guards are enforced in code so a typo cannot overload the
dev machine: event rate and batch size have hard ceilings, every run is
bounded by an explicit batch count, and the default invocation writes
at most 10_000 rows.

Usage:
    python producers/events.py --sink stdout --batch-size 5 --max-batches 2
    python producers/events.py --sink s3 --rate 50 --batch-size 500 \\
        --max-batches 20 --seed 42 --bronze-bucket bronze
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import random
import sys
import time
import uuid

MAX_RATE = 100
MAX_BATCH_SIZE = 5000
MAX_BATCHES = 100
MAX_LOOP_BATCHES = 200

DEFAULT_RATE = 10
DEFAULT_BATCH_SIZE = 500
DEFAULT_MAX_BATCHES = 20  # 500 * 20 = 10_000 rows for a bare default run.
DEFAULT_SEED = 42
DEFAULT_DUP_RATE = 0.01
DEFAULT_LATE_RATE = 0.02

SOURCE_SYSTEMS = ("web", "mobile", "pos", "api")
EVENT_TYPES = ("purchase", "view", "refund", "signup")
CURRENCIES = ("USD", "EUR", "GBP", "CHF")
COUNTRIES = ("US", "DE", "CH", "FR", "GB")
OPS = ("c", "u", "d")
OP_WEIGHTS = (0.85, 0.10, 0.05)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Synthetic Bronze event producer with hard small-scale caps."
    )
    parser.add_argument("--rate", type=float, default=DEFAULT_RATE,
                        help=f"Events per second, (0, {MAX_RATE}]. Default {DEFAULT_RATE}.")
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE,
                        help=f"Events per batch, [1, {MAX_BATCH_SIZE}]. Default {DEFAULT_BATCH_SIZE}.")
    parser.add_argument("--max-batches", type=int, default=DEFAULT_MAX_BATCHES,
                        help=f"Batch count for one-shot runs, [1, {MAX_BATCHES}]. "
                             f"Default {DEFAULT_MAX_BATCHES} (<=10k rows by default).")
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED,
                        help=f"Random seed for reproducible output. Default {DEFAULT_SEED}.")
    parser.add_argument("--dup-rate", type=float, default=DEFAULT_DUP_RATE,
                        help="Share of events duplicated within their batch, [0, 1]. "
                             f"Default {DEFAULT_DUP_RATE}.")
    parser.add_argument("--late-rate", type=float, default=DEFAULT_LATE_RATE,
                        help="Share of events backdated by 2-7 days, [0, 1]. "
                             f"Default {DEFAULT_LATE_RATE}.")
    parser.add_argument("--loop", action="store_true",
                        help="Continuous mode: batch count comes from --loop-max-batches.")
    parser.add_argument("--loop-max-batches", type=int, default=None,
                        help=f"Required with --loop: batch bound, [1, {MAX_LOOP_BATCHES}].")
    parser.add_argument("--sink", choices=("s3", "stdout"), default="s3",
                        help="Write Bronze files to S3 (default) or event JSON lines to stdout.")
    parser.add_argument("--format", choices=("json", "parquet"), default="json",
                        help="S3 object format. Default json.")
    parser.add_argument("--bronze-bucket", default="bronze",
                        help="Target bucket for --sink s3. Default bronze.")
    parser.add_argument("--s3-endpoint", default=None,
                        help="S3 endpoint URL. Defaults to S3_ENDPOINT_INTERNAL, "
                             "then S3_ENDPOINT_EXTERNAL, then http://localhost:9000.")
    parser.add_argument("--date", default=None,
                        help="dt partition (YYYY-MM-DD). Defaults to today in UTC.")
    parser.add_argument("--no-throttle", action="store_true", default=False,
                        help="Skip inter-batch rate-limit sleep for fast smoke tests. "
                             "Default runs remain throttled.")
    return parser


def validate_args(parser: argparse.ArgumentParser, args: argparse.Namespace) -> int:
    """Enforce hard caps. Returns the effective batch count."""
    if not 0 < args.rate <= MAX_RATE:
        parser.error(f"--rate must be in (0, {MAX_RATE}]")
    if not 1 <= args.batch_size <= MAX_BATCH_SIZE:
        parser.error(f"--batch-size must be in [1, {MAX_BATCH_SIZE}]")
    if not 0 <= args.dup_rate <= 1:
        parser.error("--dup-rate must be in [0, 1]")
    if not 0 <= args.late_rate <= 1:
        parser.error("--late-rate must be in [0, 1]")
    if args.format not in ("json", "parquet"):
        parser.error("--format must be json or parquet")
    if args.loop:
        if args.loop_max_batches is None:
            parser.error("--loop requires an explicit --loop-max-batches bound")
        if not 1 <= args.loop_max_batches <= MAX_LOOP_BATCHES:
            parser.error(f"--loop-max-batches must be in [1, {MAX_LOOP_BATCHES}]")
        return args.loop_max_batches
    if args.max_batches is None:
        parser.error("--max-batches is required for one-shot runs")
    if not 1 <= args.max_batches <= MAX_BATCHES:
        parser.error(f"--max-batches must be in [1, {MAX_BATCHES}]")
    return args.max_batches


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


def make_event(rng: random.Random, batch_id: str, base: dt.datetime) -> dict:
    event_time = base - dt.timedelta(seconds=rng.randint(0, 3600))
    event_type = rng.choice(EVENT_TYPES)
    amount = round(rng.uniform(1.0, 500.0), 2) if event_type in ("purchase", "refund") else 0.0
    return {
        "event_id": str(uuid.UUID(int=rng.getrandbits(128), version=4)),
        "event_time": event_time.isoformat(),
        "source_system": rng.choice(SOURCE_SYSTEMS),
        "load_time": base.isoformat(),
        "batch_id": batch_id,
        "op": rng.choices(OPS, weights=OP_WEIGHTS, k=1)[0],
        "user_id": f"user-{rng.randint(1, 5000):05d}",
        "event_type": event_type,
        "amount": amount,
        "currency": rng.choice(CURRENCIES),
        "country": rng.choice(COUNTRIES),
    }


def make_batch(rng: random.Random, batch_id: str, size: int,
               dup_rate: float, late_rate: float) -> list[dict]:
    """Build one deterministic batch, including duplicate/late injection."""
    base = dt.datetime.now(dt.timezone.utc)
    events = [make_event(rng, batch_id, base) for _ in range(size)]
    for event in events:
        if rng.random() < late_rate:
            original = dt.datetime.fromisoformat(event["event_time"])
            event["event_time"] = (original - dt.timedelta(days=rng.randint(2, 7))).isoformat()
    dups = []
    for event in events:
        if rng.random() < dup_rate:
            copy = dict(event)
            copy["load_time"] = (base + dt.timedelta(seconds=1)).isoformat()
            dups.append(copy)
    events.extend(dups)
    return events


def write_stdout(events: list[dict]) -> None:
    for event in events:
        sys.stdout.write(json.dumps(event) + "\n")


def get_s3_client(endpoint: str):
    """Build one S3 client for the given endpoint. Created once per run()."""
    try:
        import boto3
        from botocore.config import Config
    except ImportError:
        raise SystemExit("boto3 is required for --sink s3: pip install -r producers/requirements.txt")
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
        region_name=os.environ.get("AWS_REGION", "us-east-1"),
        config=Config(s3={"addressing_style": "path"}),
    )


def write_s3(events: list[dict], *, bucket: str, day: str, batch_id: str,
             index: int, fmt: str, endpoint: str, client=None) -> str:
    """Upload one batch object. Returns the s3:// key written."""
    key = f"events/dt={day}/batch={batch_id}/part-{index:04d}.{fmt}"
    if fmt == "json":
        body = ("\n".join(json.dumps(e) for e in events) + "\n").encode("utf-8")
    else:
        try:
            import io

            import pyarrow as pa
            import pyarrow.parquet as pq
        except ImportError:
            raise SystemExit("pyarrow is required for --format parquet: "
                             "pip install -r producers/requirements.txt")
        table = pa.Table.from_pylist(events)
        buf = io.BytesIO()
        pq.write_table(table, buf)
        body = buf.getvalue()
    if client is None:
        client = get_s3_client(endpoint)
    client.put_object(Bucket=bucket, Key=key, Body=body)
    return f"s3://{bucket}/{key}"


def run(args: argparse.Namespace) -> int:
    parser = build_parser()
    total_batches = validate_args(parser, args)
    try:
        day = resolve_date(args.date)
    except ValueError as exc:
        parser.error(str(exc))
    endpoint = resolve_endpoint(args.s3_endpoint)
    if args.sink == "s3" and not args.bronze_bucket:
        parser.error("--bronze-bucket must be non-empty for --sink s3")

    rng = random.Random(args.seed)
    client = get_s3_client(endpoint) if args.sink == "s3" else None
    written = 0
    for index in range(total_batches):
        batch_id = f"batch-{args.seed:05d}-{index:05d}"
        events = make_batch(rng, batch_id, args.batch_size, args.dup_rate, args.late_rate)
        if args.sink == "s3":
            key = write_s3(events, bucket=args.bronze_bucket, day=day,
                           batch_id=batch_id, index=index, fmt=args.format,
                           endpoint=endpoint, client=client)
            print(f"batch {index + 1}/{total_batches}: {len(events)} events -> {key}")
        else:
            write_stdout(events)
        written += len(events)
        if index < total_batches - 1 and args.rate > 0 and not getattr(args, "no_throttle", False):
            time.sleep(len(events) / args.rate)
    if args.sink == "s3":
        print(f"done: {written} events in {total_batches} batches")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return run(args)


if __name__ == "__main__":
    raise SystemExit(main())
