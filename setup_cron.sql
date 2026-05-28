CREATE EXTENSION IF NOT EXISTS pg_cron;

SELECT
  cron.schedule (
    'partman-maintenance',
    '0 * * * *', -- hourly
    $$CALL partman.run_maintenance_proc()$$
  );
