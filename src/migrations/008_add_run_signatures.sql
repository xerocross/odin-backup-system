-- 008_add_run_signatures.up.sql

BEGIN TRANSACTION;

-- Add four hash columns (nullable; no default). 
-- CHECK keeps us honest without forcing a specific casing.

ALTER TABLE runs
  ADD COLUMN current_upstream_signature TEXT;
ALTER TABLE runs
  ADD COLUMN previous_upstream_signature TEXT;
ALTER TABLE runs
  ADD COLUMN previous_job_signature TEXT;
ALTER TABLE runs
  ADD COLUMN job_result_signature TEXT;
-- Optional: if your migrator uses PRAGMA user_version for tracking
PRAGMA user_version = 8;

COMMIT;
