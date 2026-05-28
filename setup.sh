#!/usr/bin/env bash
# Bootstrap the stats schema end-to-end:
#   1. Create partitioned parent tables
#   2. Create indexes on the parents (so future partitions inherit them)
#   3. Register the parents with pg_partman, backfilling partitions
#      from $BACKFILL_START forward
#   4. Schedule hourly partman maintenance via pg_cron
#
# Override the historical backfill start with the BACKFILL_START env var,
# e.g. `BACKFILL_START=2024-01-01 ./setup.sh`.
set -euo pipefail

BACKFILL_START="${BACKFILL_START:-2019-01-01}"

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
run_sql="$script_dir/run-sql.sh"

echo "==> Using backfill start date: $BACKFILL_START"

"$run_sql" "$script_dir/setup_tables.sql"
"$run_sql" "$script_dir/setup_db_indices.sql"
"$run_sql" "$script_dir/setup_partitioning.sql" \
  -v "backfill_start=$BACKFILL_START"
"$run_sql" "$script_dir/setup_cron.sql"

echo "==> Setup complete."
