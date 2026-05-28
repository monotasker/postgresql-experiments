CREATE SCHEMA IF NOT EXISTS stats;

CREATE TABLE stats.record_view_events (
  -- Identity ----------------------------------------------------------------
  id bigint GENERATED ALWAYS AS IDENTITY,
  -- Mirrors the OpenSearch _id: "{timestamp}-{sha1(unique_id||visitor_id)}"
  event_id text NOT NULL,
  unique_id text NOT NULL, -- "{record_uuid}-{ident}"
  "timestamp" timestamptz(6) NOT NULL,
  UNIQUE (event_id, "timestamp"),
  UNIQUE (unique_id, "timestamp"),
  updated_timestamp timestamptz(6),
  -- Visitor / session -------------------------------------------------------
  visitor_id text NOT NULL, -- anonymised hash
  unique_session_id text NOT NULL,
  is_machine boolean,
  is_robot boolean,
  -- Request context (consider dropping ip / user_agent after anonymisation)-
  country text, -- ISO 3166-1 alpha-2
  referrer text,
  via_api boolean,
  labels text[],
  -- Stable IDs (no FK, so events survive deletion of the referenced row) ----
  record_id uuid NOT NULL, -- rdm_records_metadata.id (version)
  recid text NOT NULL, -- PID of the version
  parent_id uuid NOT NULL, -- rdm_parents_metadata.id
  parent_recid text NOT NULL, -- PID of the parent (concept)
  -- Snapshotted scalar metadata (hot filter columns) -----------------------
  community_ids uuid[], -- communities at event time
  access_status text NOT NULL,
  publisher text,
  journal_title text,
  resource_type_id text, -- e.g. "publication-article"
  file_types text[],
  -- Snapshotted nested / multi-valued metadata -----------------------------
  -- Keep the OS structure intact: subjects[], languages[], rights[],
  -- affiliations[], funders[], full resource_type{} with i18n titles.
  record_metadata_snapshot jsonb NOT NULL DEFAULT '{}'::jsonb
)
-- Monthly partitions; create ahead with pg_partman or a cron job
-- CREATE TABLE stats.record_view_events_2026_05 PARTITION OF stats.record_view_events
--     FOR VALUES FROM ('2026-05-01') TO ('2026-06-01');
PARTITION BY
  RANGE ("timestamp");

CREATE TABLE stats.file_download_events (
  -- all the shared columns above
  LIKE stats.record_view_events INCLUDING ALL,
  -- File-specific -----------------------------------------------------------
  bucket_id uuid NOT NULL, -- files_bucket.id
  file_id uuid NOT NULL, -- files_files.id
  file_key text NOT NULL, -- filename within the bucket
  size bigint -- bytes
)
PARTITION BY
  RANGE ("timestamp");
