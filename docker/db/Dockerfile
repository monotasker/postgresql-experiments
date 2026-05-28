# Postgres 14 + pg_partman (for partition mgmt) + pg_cron (for scheduling).
#
# Both extensions live in the PGDG apt repository that the official postgres
# image already trusts, so we just install the matching server-version
# packages. pg_cron additionally needs to be in shared_preload_libraries;
# that is configured via `command:` in docker-compose.yml, not here.
#
# NOTE: Postgres 14 matches the upstream InvenioRDM project template and the
# shared CI workflow used by invenio-db / invenio-rdm-records.
FROM postgres:14

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        postgresql-14-partman \
        postgresql-14-cron \
    && rm -rf /var/lib/apt/lists/*
