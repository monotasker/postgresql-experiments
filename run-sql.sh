#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Load POSTGRES_USER / POSTGRES_PASSWORD / POSTGRES_DB from .env so we can
# forward credentials to psql. docker-compose reads .env on its own, but a
# plain shell does not, so without this psql has no password to send and
# fails with `fe_sendauth: no password supplied`.
if [[ -f "$script_dir/.env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "$script_dir/.env"
  set +a
fi

PGHOST="${PGHOST:-localhost}"
PGPORT="${PGPORT:-5434}"
PGUSER="${PGUSER:-${POSTGRES_USER:-ianscott}}"
PGDATABASE="${PGDATABASE:-${POSTGRES_DB:-learning}}"
PGPASSWORD="${PGPASSWORD:-${POSTGRES_PASSWORD:-}}"

if [[ -z "$PGPASSWORD" ]]; then
  echo "run-sql.sh: no password found (set POSTGRES_PASSWORD in .env or PGPASSWORD)" >&2
  exit 2
fi

export PGHOST PGPORT PGUSER PGDATABASE PGPASSWORD

if [[ $# -lt 1 ]]; then
  echo "usage: $(basename "$0") <script.sql> [psql args...]" >&2
  exit 2
fi

sql_file="$1"
shift

exec psql -X -v ON_ERROR_STOP=1 -1 \
  -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" \
  -f "$sql_file" "$@"
