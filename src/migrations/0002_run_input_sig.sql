
-- 0002_run_input_sig.sql
PRAGMA foreign_keys=ON;

-- Add run-level input signature
ALTER TABLE runs ADD COLUMN input_sig_json TEXT;
ALTER TABLE runs ADD COLUMN output_sig_json TEXT;

-- Backfill from earliest step.input_sig_json per run (if present)
UPDATE runs
SET input_sig_json = (
  SELECT s.input_sig_json
  FROM steps s
  WHERE s.run_id = runs.run_id AND s.input_sig_json IS NOT NULL
  ORDER BY s.started_at ASC
  LIMIT 1
)
WHERE input_sig_json IS NULL;

-- Backfill from earliest step.output_sig_json per run (if present)
UPDATE runs
SET output_sig_json = (
  SELECT s.output_sig_json
  FROM steps s
  WHERE s.run_id = runs.run_id AND s.output_sig_json IS NOT NULL
  ORDER BY s.started_at ASC
  LIMIT 1
)
WHERE output_sig_json IS NULL;


-- Try to drop the column if your SQLite supports DROP COLUMN (>=3.35.0)
-- If it doesn't, comment the DROP and use the rebuild block below.
--
ALTER TABLE steps DROP COLUMN input_sig_json;
ALTER TABLE steps DROP COLUMN output_sig_json;
