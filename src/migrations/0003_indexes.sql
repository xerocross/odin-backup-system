
-- 0003_indexes.sql
PRAGMA foreign_keys=ON;
CREATE INDEX IF NOT EXISTS idx_runs_status ON runs(status);
CREATE INDEX IF NOT EXISTS idx_runs_started ON runs(started_at);
CREATE INDEX IF NOT EXISTS idx_steps_status ON steps(status);
