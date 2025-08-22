
-- 0001_init.sql
PRAGMA foreign_keys=ON;
PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS runs (
  run_id      TEXT PRIMARY KEY,
  started_at  INTEGER NOT NULL,
  finished_at INTEGER,
  status      TEXT CHECK(status IN ('running','success','failed')) NOT NULL,
  meta_json   TEXT
);

CREATE TABLE IF NOT EXISTS steps (
  id              INTEGER PRIMARY KEY AUTOINCREMENT,
  run_id          TEXT NOT NULL,
  name            TEXT NOT NULL,
  started_at      INTEGER NOT NULL,
  finished_at     INTEGER,
  status          TEXT CHECK(status IN ('running','success','failed','skipped')) NOT NULL,
  message         TEXT,
  input_sig_json  TEXT,    -- e.g. {"tar_sha256": "...", "recipient": "..."}
  output_path     TEXT,
  output_sig_json TEXT,    -- e.g. {"sha256": "..."}
  FOREIGN KEY(run_id) REFERENCES runs(run_id)
);

CREATE INDEX IF NOT EXISTS idx_steps_run ON steps(run_id);
