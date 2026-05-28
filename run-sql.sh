#!/usr/bin/env bash
set -euo pipefail

PGHOST="${PGHOST:-localhost}"
PGPORT="${PGPORT:-5434}"
PGUSER="${POSTGRES_USER:-ianscott}"
PGDATABASE="${POSTGRES_DB:-learning}"

if [[ $# -lt 1 ]]; then
  echo "usage: $(basename "$0") <script.sql> [psql args...]" >&2
  exit 2
fi

sql_file="$1"
shift

exec psql -X -v ON_ERROR_STOP=1 -1 \
  -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" \
  -f "$sql_file" "$@"

export PGHOST PGPORT PGUSER PGDATABASE
