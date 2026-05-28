#!/usr/bin/env python3
"""Bulk-generate sample events for `stats.record_view_events` and `stats.file_download_events`.

Uses psycopg3 binary ``COPY FROM STDIN`` across multiple worker processes for maximum
throughput. Designed to insert millions of rows in seconds against a local Postgres.

Usage
-----
    # Make sure dependencies are installed:
    uv sync

    # Insert 1M rows (default: 80% views / 20% downloads):
    uv run python seed_data.py --rows 1_000_000

    # 5M views only, 12 workers, 50k batches:
    uv run python seed_data.py --rows 5_000_000 --table views --workers 12 --batch-size 50000

    # Wipe existing data first:
    uv run python seed_data.py --rows 1_000_000 --truncate

Notes
-----
* Connection details are read from the project's ``.env`` (POSTGRES_USER / PASSWORD / DB)
  with PGHOST/PGPORT overrides; or pass ``--dsn postgresql://...`` explicitly.
* Realism is shallow on purpose: small pools of countries / publishers / subjects are
  sampled per event, and each "record" (identified by a stable UUID) carries a fixed
  snapshot so repeated events for the same record look consistent.
"""

from __future__ import annotations

import argparse
import multiprocessing as mp
import os
import random
import sys
import time
import uuid
from collections.abc import Iterator
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import psycopg
from dotenv import load_dotenv
from psycopg.types.json import Jsonb

# Row payloads have a variable length (view rows vs. download rows) and contain
# many disparate scalar / array / JSONB values, so we type them loosely.
Row = tuple[Any, ...]
Pools = dict[str, Any]
WorkerArgs = tuple[
    int, str, str, int, int, float, float, dict[str, int], int
]

REPO_ROOT = Path(__file__).resolve().parent
ENV_PATH = REPO_ROOT / ".env"

# ---------------------------------------------------------------------------
# Pre-defined value pools. Small enough that random.choice is O(1) and cheap;
# big enough that the resulting data has reasonable cardinality for indexing
# experiments. Extend freely.
# ---------------------------------------------------------------------------
COUNTRIES = [
    "US", "GB", "DE", "FR", "JP", "CA", "AU", "BR", "IN", "CN", "ES", "IT",
    "NL", "SE", "CH", "NO", "FI", "DK", "BE", "AT", "PL", "MX", "KR", "ZA", "SG",
]
LANGUAGES = [
    "en", "fr", "de", "es", "it", "ja", "zh", "ru", "pt", "nl",
    "ar", "ko", "sv", "no", "fi", "da", "tr", "pl", "cs", "hu",
]
ACCESS_STATUSES = ["open", "restricted", "embargoed", "metadata-only"]
ACCESS_STATUS_WEIGHTS = [70, 10, 10, 10]
RESOURCE_TYPES = [
    "publication-article", "publication-book", "publication-conferencepaper",
    "publication-deliverable", "publication-report", "publication-preprint",
    "publication-thesis", "publication-workingpaper", "dataset", "software",
    "image-figure", "image-photo", "image-drawing", "video", "audio",
    "poster", "presentation", "lesson", "other",
]
FILE_TYPES = [
    "pdf", "csv", "zip", "txt", "jpg", "png", "mp4", "tar", "json",
    "xml", "docx", "xlsx", "gz", "tiff", "wav",
]
REFERRERS = [
    "https://www.google.com/", "https://scholar.google.com/",
    "https://twitter.com/", "https://www.semanticscholar.org/",
    "https://duckduckgo.com/", "https://www.bing.com/",
    "https://en.wikipedia.org/", "https://github.com/",
    "https://orcid.org/", "https://www.nature.com/",
    None, None, None, None,  # extra Nones bias toward NULL referrers
]
PUBLISHERS = [
    "Zenodo", "Elsevier", "Springer Nature", "Wiley", "MDPI", "PLoS",
    "arXiv", "CERN", "NASA", "NOAA", "CNRS", "DESY", "IEEE", "ACM",
]
JOURNAL_TITLES = [
    "Nature", "Science", "Cell", "PLoS One", "JBC", "JCAP", "JHEP",
    "PRD", "PRL", "PNAS", "RSC", "BMJ", "arXiv preprint", "JOSS",
]
LABELS = [
    "new", "featured", "editor-pick", "trending", "preprint",
    "peer-reviewed", "open-access", "cc-by", "cc0",
]
SUBJECTS = [
    "physics", "mathematics", "biology", "chemistry", "computer-science",
    "medicine", "economics", "sociology", "psychology", "engineering",
    "statistics", "astronomy", "neuroscience", "ecology", "linguistics",
    "philosophy", "history", "education",
]
AFFILIATIONS = [
    "CERN", "MIT", "Stanford", "Harvard", "Oxford", "Cambridge", "ETH Zurich",
    "Max Planck", "CNRS", "INRIA", "DESY", "Fermilab", "UCL", "TU Munich",
    "Imperial College", "KAIST", "Tokyo University", "Tsinghua", "Princeton",
    "Yale", "Caltech", "UCLA",
]
FUNDERS = [
    "NSF", "NIH", "ERC", "DFG", "CNRS", "Wellcome Trust",
    "Gates Foundation", "DOE", "NASA", "JSPS", "UKRI",
]
RIGHTS = [
    {"id": "cc-by-4.0"},
    {"id": "cc0-1.0"},
    {"id": "cc-by-sa-4.0"},
    {"id": "cc-by-nc-4.0"},
    {"id": "mit"},
    {"id": "apache-2.0"},
]


# Columns are listed in COPY order. ``timestamp`` is a reserved word, so we
# always emit it quoted.
VIEW_COLUMNS: tuple[str, ...] = (
    "event_id", "unique_id", '"timestamp"', "updated_timestamp",
    "visitor_id", "unique_session_id", "is_machine", "is_robot",
    "country", "referrer", "via_api", "labels",
    "record_id", "recid", "parent_id", "parent_recid",
    "community_ids", "access_status", "publisher", "journal_title",
    "resource_type_id", "file_types", "record_metadata_snapshot",
)
DOWNLOAD_COLUMNS: tuple[str, ...] = VIEW_COLUMNS + (
    "bucket_id", "file_id", "file_key", "size",
)


# ---------------------------------------------------------------------------
# Pool construction. Built deterministically from --seed so each worker can
# rebuild the same pools after a process spawn without sharing state.
# ---------------------------------------------------------------------------

def _uuid_from(rng: random.Random) -> uuid.UUID:
    return uuid.UUID(int=rng.getrandbits(128))


def build_pools(
    *,
    num_records: int,
    num_visitors: int,
    num_communities: int,
    num_buckets: int,
    seed: int,
) -> Pools:
    """Pre-compute every value that's shared across many events.

    Per-record properties (publisher, snapshot, etc.) are pinned to a record
    index, so repeated events for the same record look self-consistent.
    """
    rng = random.Random(seed)

    parent_ids = [_uuid_from(rng) for _ in range(num_records)]
    record_ids = [_uuid_from(rng) for _ in range(num_records)]
    # Two PID pools so a record's parent_recid (concept) is distinct from its recid (version)
    parent_recids = [str(rng.randrange(10_000, 9_999_999)) for _ in range(num_records)]
    recids = [str(rng.randrange(10_000, 9_999_999)) for _ in range(num_records)]

    visitor_ids = [rng.randbytes(16).hex() for _ in range(num_visitors)]
    # ~3 sessions per visitor on average
    session_ids = [rng.randbytes(16).hex() for _ in range(max(num_visitors * 3, num_visitors))]
    community_ids_pool = [_uuid_from(rng) for _ in range(num_communities)]
    bucket_ids = [_uuid_from(rng) for _ in range(max(num_buckets, 1))]

    # Per-record fixed metadata. Stored as parallel lists indexed by record_idx
    # for fast lookup (no dict hashing in the hot loop).
    rec_access = [
        rng.choices(ACCESS_STATUSES, weights=ACCESS_STATUS_WEIGHTS, k=1)[0]
        for _ in range(num_records)
    ]
    rec_publisher = [
        rng.choice(PUBLISHERS) if rng.random() > 0.20 else None
        for _ in range(num_records)
    ]
    rec_journal = [
        rng.choice(JOURNAL_TITLES) if rng.random() > 0.50 else None
        for _ in range(num_records)
    ]
    rec_resource_type = [rng.choice(RESOURCE_TYPES) for _ in range(num_records)]
    rec_file_types = [
        rng.sample(FILE_TYPES, k=rng.randint(1, 3)) for _ in range(num_records)
    ]
    rec_communities: list[list[uuid.UUID]] = [
        rng.sample(community_ids_pool, k=rng.choices([0, 1, 2, 3], weights=[5, 60, 30, 5])[0])
        for _ in range(num_records)
    ]
    # Snapshot dicts -> pre-wrap with Jsonb so psycopg knows the target type.
    rec_snapshot: list[Jsonb] = []
    for rt in rec_resource_type:
        snapshot = {
            "subjects": rng.sample(SUBJECTS, k=rng.randint(1, 4)),
            "languages": rng.sample(LANGUAGES, k=rng.choices([1, 2], weights=[85, 15])[0]),
            "rights": [rng.choice(RIGHTS)],
            "affiliations": rng.sample(AFFILIATIONS, k=rng.randint(0, 3)),
            "funders": rng.sample(FUNDERS, k=rng.choices([0, 1, 2], weights=[60, 30, 10])[0]),
            "resource_type": {
                "id": rt,
                "title": {"en": rt.replace("-", " ").title()},
            },
        }
        rec_snapshot.append(Jsonb(snapshot))

    return {
        "parent_ids": parent_ids,
        "record_ids": record_ids,
        "parent_recids": parent_recids,
        "recids": recids,
        "visitor_ids": visitor_ids,
        "session_ids": session_ids,
        "community_ids_pool": community_ids_pool,
        "bucket_ids": bucket_ids,
        "rec_access": rec_access,
        "rec_publisher": rec_publisher,
        "rec_journal": rec_journal,
        "rec_resource_type": rec_resource_type,
        "rec_file_types": rec_file_types,
        "rec_communities": rec_communities,
        "rec_snapshot": rec_snapshot,
    }


# ---------------------------------------------------------------------------
# Row generation. Hot loop -- keep this lean.
# ---------------------------------------------------------------------------

def _generate_rows(
    *,
    rng: random.Random,
    pools: Pools,
    count: int,
    start_epoch: float,
    span_seconds: float,
    worker_id: int,
    start_counter: int,
    with_file: bool,
) -> Iterator[Row]:
    """Yield ``count`` rows as tuples ready for ``copy.write_row``."""
    parent_ids = pools["parent_ids"]
    record_ids = pools["record_ids"]
    parent_recids = pools["parent_recids"]
    recids = pools["recids"]
    visitor_ids = pools["visitor_ids"]
    session_ids = pools["session_ids"]
    bucket_ids = pools["bucket_ids"]
    rec_access = pools["rec_access"]
    rec_publisher = pools["rec_publisher"]
    rec_journal = pools["rec_journal"]
    rec_resource_type = pools["rec_resource_type"]
    rec_file_types = pools["rec_file_types"]
    rec_communities = pools["rec_communities"]
    rec_snapshot = pools["rec_snapshot"]

    n_records = len(parent_ids)
    n_visitors = len(visitor_ids)
    n_sessions = len(session_ids)
    n_buckets = len(bucket_ids)

    worker_hex = f"{worker_id:04x}"
    rand = rng.random
    randint = rng.randint
    randrange = rng.randrange
    choice = rng.choice
    sample = rng.sample
    choices = rng.choices
    lognormvariate = rng.lognormvariate
    getrandbits = rng.getrandbits

    counter = start_counter
    for _ in range(count):
        ts_epoch = start_epoch + rand() * span_seconds
        ts = datetime.fromtimestamp(ts_epoch, tz=timezone.utc)

        rec_idx = randrange(n_records)
        record_id = record_ids[rec_idx]

        # Suffix is globally unique across all workers because (worker_id, counter)
        # is unique, even if two events land on identical timestamps.
        suffix = f"{worker_hex}{counter:012x}"
        ts_us = int(ts_epoch * 1_000_000)
        event_id = f"{ts_us}-{suffix}"
        unique_id = f"{record_id.hex}-{suffix}"

        updated_timestamp = None
        if rand() < 0.05:
            updated_timestamp = ts + timedelta(seconds=randint(1, 86400))

        is_machine = rand() < 0.10
        is_robot = is_machine and rand() < 0.30
        country = choice(COUNTRIES) if rand() > 0.05 else None
        referrer = choice(REFERRERS)
        via_api = rand() < 0.15

        n_labels = choices((0, 1, 2, 3), weights=(60, 25, 10, 5), k=1)[0]
        labels = sample(LABELS, k=n_labels) if n_labels else []

        row: Row = (
            event_id,
            unique_id,
            ts,
            updated_timestamp,
            visitor_ids[randrange(n_visitors)],
            session_ids[randrange(n_sessions)],
            is_machine,
            is_robot,
            country,
            referrer,
            via_api,
            labels,
            record_id,
            recids[rec_idx],
            parent_ids[rec_idx],
            parent_recids[rec_idx],
            rec_communities[rec_idx],
            rec_access[rec_idx],
            rec_publisher[rec_idx],
            rec_journal[rec_idx],
            rec_resource_type[rec_idx],
            rec_file_types[rec_idx],
            rec_snapshot[rec_idx],
        )

        if with_file:
            bucket_id = bucket_ids[randrange(n_buckets)]
            file_id = uuid.UUID(int=getrandbits(128))
            ext = choice(rec_file_types[rec_idx]) if rec_file_types[rec_idx] else "bin"
            file_key = f"data_{randrange(1, 1000):03d}.{ext}"
            # log-normal size distribution: median ~ e^13 bytes (~440KB), long tail
            size = int(lognormvariate(13.0, 1.8))
            row = row + (bucket_id, file_id, file_key, size)

        counter += 1
        yield row


# ---------------------------------------------------------------------------
# Worker entrypoint
# ---------------------------------------------------------------------------

def _copy_sql(table: str) -> str:
    cols = DOWNLOAD_COLUMNS if table == "downloads" else VIEW_COLUMNS
    qualified = (
        "stats.file_download_events" if table == "downloads" else "stats.record_view_events"
    )
    cols_sql = ", ".join(cols)
    return f"COPY {qualified} ({cols_sql}) FROM STDIN (FORMAT BINARY)"


def worker(args_tuple: WorkerArgs) -> tuple[int, str, int, float]:
    (
        worker_id,
        dsn,
        table,
        num_rows,
        batch_size,
        start_epoch,
        span_seconds,
        pools_meta,
        seed,
    ) = args_tuple

    # Each worker gets a deterministic but distinct PRNG sequence.
    rng = random.Random(seed * 1_000_003 + worker_id)
    # Pools are rebuilt from the *base* seed so every worker shares the same
    # record/visitor universe.
    pools = build_pools(**pools_meta, seed=seed)

    copy_sql = _copy_sql(table)
    written = 0
    counter = worker_id * 10**14  # plenty of headroom; keeps event_ids distinct per worker
    start = time.perf_counter()

    with psycopg.connect(dsn, autocommit=False) as conn:
        with conn.cursor() as cur:
            cur.execute("SET synchronous_commit = off")
            cur.execute("SET LOCAL statement_timeout = 0")
        while written < num_rows:
            this_batch = min(batch_size, num_rows - written)
            with conn.cursor() as cur, cur.copy(copy_sql) as cp:
                for row in _generate_rows(
                    rng=rng,
                    pools=pools,
                    count=this_batch,
                    start_epoch=start_epoch,
                    span_seconds=span_seconds,
                    worker_id=worker_id,
                    start_counter=counter,
                    with_file=(table == "downloads"),
                ):
                    cp.write_row(row)
            conn.commit()
            counter += this_batch
            written += this_batch
            elapsed = time.perf_counter() - start
            rate = written / elapsed if elapsed > 0 else 0.0
            print(
                f"[w{worker_id:02d}/{table:<9}] {written:>12,} / {num_rows:,} "
                f"({rate:>10,.0f} rows/s)",
                flush=True,
            )

    return worker_id, table, written, time.perf_counter() - start


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def build_dsn(args: argparse.Namespace) -> str:
    if args.dsn:
        return str(args.dsn)
    load_dotenv(ENV_PATH, override=False)
    user = os.getenv("POSTGRES_USER", "ianscott")
    pw = os.getenv("POSTGRES_PASSWORD", "abc123")
    db = os.getenv("POSTGRES_DB", "learning")
    host = os.getenv("PGHOST", "localhost")
    port = os.getenv("PGPORT", "5434")
    return f"postgresql://{user}:{pw}@{host}:{port}/{db}"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Bulk-generate sample stats events into the partitioned tables.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("--rows", type=int, default=1_000_000, help="Total rows to insert.")
    p.add_argument(
        "--workers", type=int, default=os.cpu_count() or 4,
        help="Number of concurrent COPY workers per table.",
    )
    p.add_argument("--batch-size", type=int, default=20_000, help="Rows per COPY batch / commit.")
    p.add_argument(
        "--table", choices=("both", "views", "downloads"), default="both",
        help="Which table(s) to populate.",
    )
    p.add_argument(
        "--views-ratio", type=float, default=0.8,
        help="Fraction of rows that go into record_view_events when --table=both.",
    )
    p.add_argument("--start", default="2019-01-01", help="Earliest event timestamp (UTC).")
    p.add_argument("--end", default=None, help="Latest event timestamp (UTC, default = now).")
    p.add_argument(
        "--num-records", type=int, default=10_000,
        help="Size of the synthetic record universe (more = lower per-record event count).",
    )
    p.add_argument("--num-visitors", type=int, default=100_000)
    p.add_argument("--num-communities", type=int, default=50)
    p.add_argument("--num-buckets", type=int, default=5_000)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument(
        "--truncate", action="store_true",
        help="TRUNCATE the target tables (cascades into partitions) before loading.",
    )
    p.add_argument(
        "--dsn", default=None,
        help="psycopg connection URL; overrides .env-derived defaults.",
    )
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    dsn = build_dsn(args)

    start_dt = datetime.fromisoformat(args.start).replace(tzinfo=timezone.utc)
    end_dt = (
        datetime.fromisoformat(args.end).replace(tzinfo=timezone.utc)
        if args.end else datetime.now(tz=timezone.utc)
    )
    if end_dt <= start_dt:
        print("error: --end must be after --start", file=sys.stderr)
        return 2

    start_epoch = start_dt.timestamp()
    span_seconds = (end_dt - start_dt).total_seconds()

    pools_meta = {
        "num_records": args.num_records,
        "num_visitors": args.num_visitors,
        "num_communities": args.num_communities,
        "num_buckets": args.num_buckets,
    }

    if args.truncate:
        print("Truncating target tables...", flush=True)
        with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
            if args.table in ("views", "both"):
                cur.execute("TRUNCATE TABLE stats.record_view_events")
            if args.table in ("downloads", "both"):
                cur.execute("TRUNCATE TABLE stats.file_download_events")

    # Split totals between tables, then between workers within each table.
    tables: list[tuple[str, int]]
    if args.table == "both":
        views_total = int(args.rows * args.views_ratio)
        downloads_total = args.rows - views_total
        tables = [("views", views_total), ("downloads", downloads_total)]
    elif args.table == "views":
        tables = [("views", args.rows)]
    else:
        tables = [("downloads", args.rows)]

    worker_args: list[WorkerArgs] = []
    wid = 0
    for table, total in tables:
        if total <= 0:
            continue
        per, rem = divmod(total, args.workers)
        for w in range(args.workers):
            count = per + (1 if w < rem else 0)
            if count == 0:
                continue
            worker_args.append((
                wid, dsn, table, count, args.batch_size,
                start_epoch, span_seconds, pools_meta, args.seed,
            ))
            wid += 1

    if not worker_args:
        print("Nothing to do (rows=0?).")
        return 0

    print(
        f"Spawning {len(worker_args)} worker(s) to insert {args.rows:,} row(s) "
        f"into {args.table} between {start_dt.date()} and {end_dt.date()}...",
        flush=True,
    )
    overall_start = time.perf_counter()

    ctx = mp.get_context("spawn")
    try:
        with ctx.Pool(processes=len(worker_args)) as pool:
            results = pool.map(worker, worker_args)
    except KeyboardInterrupt:
        print("\nInterrupted. Some rows may be committed already.", file=sys.stderr)
        return 130

    overall_elapsed = time.perf_counter() - overall_start
    total_written = sum(r[2] for r in results)
    print(
        f"\nDone. Wrote {total_written:,} row(s) across {len(results)} worker(s) "
        f"in {overall_elapsed:.1f}s "
        f"({total_written / overall_elapsed:,.0f} rows/s overall)."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
