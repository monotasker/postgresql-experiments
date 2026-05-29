CREATE SCHEMA IF NOT EXISTS partman;

CREATE EXTENSION IF NOT EXISTS pg_partman SCHEMA partman;

SELECT
  partman.create_parent (
    p_parent_table => 'stats1.record_view_events',
    p_control => 'timestamp',
    p_type => 'range',
    p_interval => '1 month',
    p_premake => 4,
    p_start_partition =>:'backfill_start'
  );

SELECT
  partman.create_parent (
    p_parent_table => 'stats1.file_download_events',
    p_control => 'timestamp',
    p_type => 'range',
    p_interval => '1 month',
    p_premake => 4,
    p_start_partition =>:'backfill_start'
  );

-- Sanity checks for reference
-- See the registration
-- SELECT parent_table, partition_interval, premake, retention
--   FROM partman.part_config;
-- See actual partitions
-- SELECT inhrelid::regclass AS partition
--   FROM pg_inherits
--  WHERE inhparent = 'stats1.record_view_events'::regclass
--  ORDER BY 1;
