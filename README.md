# postgresql-experiments

An experimental workspace for poking at a partitioned PostgreSQL stats schema
(`stats.record_view_events`, `stats.file_download_events`) using
[SQLAlchemy 2.x](https://docs.sqlalchemy.org/en/20/) and
[psycopg 3](https://www.psycopg.org/psycopg3/).

The Postgres instance itself is provisioned via `docker-compose.yml` and the
SQL files at the repo root (`setup_tables.sql`, `setup_db_indices.sql`,
`setup_partitioning.sql`, `setup_cron.sql`).

## Prerequisites

- [Docker](https://www.docker.com/) (for the local Postgres + pgAdmin stack)
- [uv](https://docs.astral.sh/uv/) for Python environment management
- Python 3.12 (uv can install it for you)

## Bring up the database

```bash
# Postgres on localhost:5434, pgAdmin on localhost:5054
docker compose up -d

# Run the schema bootstrap scripts
./setup.sh
```

The compose stack publishes two services:

- **Postgres 14** on `localhost:5434` (mapped from the container's internal `5432`),
  built with `pg_partman` and `pg_cron` for partition maintenance.
- **pgAdmin 4** on `localhost:5054`.

### Accessing pgAdmin

The `dpage/pgadmin4` image ships with its own embedded web server, so **no
separate web server is required**. Once `docker compose up -d` is running,
open:

```text
http://localhost:5054
```

Login credentials come from your `.env`:

- `PGADMIN_DEFAULT_EMAIL`
- `PGADMIN_DEFAULT_PASSWORD`

The Postgres server connection is preconfigured inside pgAdmin via
`docker/pgadmin/servers.json`, which is mounted into the container at
`/pgadmin4/servers.json`. You should see the local Postgres server already
listed in the left-hand tree on first login — just expand it and supply the
DB password from `.env` when prompted.

If you need to confirm both containers are healthy:

```bash
docker compose ps
```

## Python environment

This is a [uv](https://docs.astral.sh/uv/)-managed project. The virtualenv
lives at `./.venv` and is created automatically.

```bash
# Pin the interpreter (uv will download 3.12 if needed)
uv python pin 3.12

# Install core deps + the `dev` dependency group
uv sync

# Add the interactive / fixture-generation extras when you want them
uv sync --extra notebook --extra fixtures

# Run something inside the venv without activating it
uv run python -c "import sqlalchemy; print(sqlalchemy.__version__)"
uv run alembic --help
uv run jupyter lab          # requires --extra notebook
```

To activate the venv directly instead of using `uv run`:

```bash
source .venv/bin/activate
```

## What's installed

Core dependencies (always):

- `SQLAlchemy[asyncio]` - ORM + Core, with async support for the partitioned tables.
- `psycopg[binary,pool]` - modern PostgreSQL driver, sync + async, with a connection pool.
- `alembic` - schema migrations, useful while iterating on partitioning/indices.
- `python-dotenv` - reads connection settings from the existing `.env` file.
- `pydantic` - typed models for the JSONB `record_metadata_snapshot` payload.

Optional extras:

- `notebook` — JupyterLab, IPython, pandas, matplotlib for interactive exploration.
- `fixtures` — `faker` for generating synthetic events to stress-test partitions / indices.

Dependency group (installed by default with `uv sync`):

- `dev` — `ruff`, `mypy` (+ SQLAlchemy plugin), `pytest`, `pytest-asyncio`.

## Generate sample data

`seed_data.py` bulk-loads synthetic events into the two partitioned tables. It
uses `psycopg 3`'s binary `COPY FROM STDIN` across multiple worker processes,
which is typically 10–100× faster than `INSERT`-based fixtures and comfortably
inserts millions of rows in seconds against a local Docker Postgres.

```bash
# Quick smoke test (1M rows split 80/20 between views and downloads):
uv run python seed_data.py --rows 1_000_000

# 10M rows, downloads only, over a custom window:
uv run python seed_data.py --table downloads --rows 10_000_000 \
    --start 2024-01-01 --end 2026-06-01

# Wipe existing data first; larger record/visitor universe for higher cardinality:
uv run python seed_data.py --rows 5_000_000 --truncate \
    --num-records 100_000 --num-visitors 1_000_000

# Point at a non-default DSN (otherwise read from .env + localhost:5434):
uv run python seed_data.py --rows 100_000 \
    --dsn postgresql://user:pw@host:5432/db
```

### How it works

- Spawns `--workers` processes per table (default = `os.cpu_count()`), each
  holding its own connection and streaming rows via
  `COPY ... FROM STDIN (FORMAT BINARY)`.
- Each session runs with `synchronous_commit = off` so per-batch commits don't
  wait for WAL fsync. Safe for ephemeral test data; **don't** copy that setting
  into anything production-bound.
- Realistic-ish values are sampled from small in-memory pools (countries,
  publishers, subjects, resource types, etc.) — no `faker` calls in the hot
  loop. Per-record metadata (publisher, snapshot JSONB, resource type, …) is
  pinned to a fixed pool of synthetic records, so repeated events for the same
  record stay self-consistent.
- `event_id` / `unique_id` are derived from `(timestamp, worker_id, counter)`
  so the `UNIQUE (event_id, "timestamp")` and `UNIQUE (unique_id, "timestamp")`
  constraints never collide, even across workers.

### Flags worth knowing

| Flag | Default | Notes |
| --- | --- | --- |
| `--rows` | `1_000_000` | Total rows across all selected tables. |
| `--table` | `both` | `views`, `downloads`, or `both`. |
| `--views-ratio` | `0.8` | Split when `--table=both`. |
| `--workers` | `os.cpu_count()` | Concurrent COPY streams per table. |
| `--batch-size` | `20_000` | Rows per COPY / per commit. |
| `--start` / `--end` | `2019-01-01` / now | Timestamp window (UTC, ISO 8601). |
| `--num-records` | `10_000` | Size of synthetic record universe. |
| `--num-visitors` | `100_000` | Size of synthetic visitor pool. |
| `--num-communities` | `50` | Pool of community UUIDs. |
| `--num-buckets` | `5_000` | Pool of file-bucket UUIDs (downloads only). |
| `--seed` | `42` | Deterministic pool generation. |
| `--truncate` | off | `TRUNCATE` the target table(s) before loading. |
| `--dsn` | from `.env` | Overrides the connection URL entirely. |

Progress prints per worker per batch, e.g.

```text
[w03/views    ]      300,000 / 800,000 (    187,500 rows/s)
```

### Tips for very large loads

- The `--start` / `--end` window must fall inside ranges that `pg_partman` has
  already pre-created. The defaults match `BACKFILL_START=2019-01-01` from
  `setup.sh`; if you push `--end` further into the future, run
  `CALL partman.run_maintenance_proc()` (or wait for the hourly cron job from
  `setup_cron.sql`) first so the partitions exist.
- For the absolute fastest load on tens of millions of rows, drop the GIN /
  BRIN indexes from `setup_db_indices.sql` before running the script and
  re-create them afterward — index maintenance dominates COPY time once the
  tables are non-trivially sized.
- `--workers` higher than the Postgres `max_connections` / CPU count won't
  help; on a laptop, `os.cpu_count()` workers per table is already plenty.

## Layout

```text
.
├── docker-compose.yml      # Postgres 14 + pgAdmin
├── docker/db/Dockerfile    # Postgres 14 + pg_partman + pg_cron
├── .env                    # DB credentials (gitignored)
├── setup.sh                # Apply all *.sql files in order
├── setup_tables.sql        # stats.record_view_events / file_download_events
├── setup_db_indices.sql
├── setup_partitioning.sql
├── setup_cron.sql
├── seed_data.py            # Bulk-generate synthetic events via COPY
├── pyproject.toml          # Python project + uv config
└── .venv/                  # Local virtualenv (gitignored)
```
