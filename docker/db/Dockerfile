# Postgres 14 + pg_partman (for partition mgmt) + pg_cron (for scheduling).
#
# pg_cron additionally needs to be in shared_preload_libraries;
# that is configured via `command:` in docker-compose.yml, not here.
#
FROM postgres:14

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        postgresql-14-partman \
        postgresql-14-cron \
    && rm -rf /var/lib/apt/lists/*
