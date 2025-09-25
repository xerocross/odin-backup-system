DROP INDEX IF EXISTS idx_runs_status;
DROP INDEX IF EXISTS idx_runs_started;
DROP INDEX IF EXISTS idx_steps_status;

CREATE TABLE runs_new (
          run_id            TEXT PRIMARY KEY,
          name              TEXT,
          started_at        INTEGER NOT NULL,
          finished_at       INTEGER,
          status            TEXT CHECK(status IN ('running','success','failed','skipped')) NOT NULL,
          meta_json         TEXT,
          input_sig_json    TEXT,
          input_sig_hash    TEXT,
          output_sig_json   TEXT,
          output_sig_hash   TEXT,
          output_path       TEXT
        );

INSERT INTO runs_new (run_id, started_at, finished_at, status, meta_json, 
                 name, input_sig_json, input_sig_hash, output_sig_json, output_sig_hash, output_path)
        SELECT run_id, started_at, finished_at, status, meta_json, name, input_sig_json, 
                 input_sig_hash, output_sig_json, output_sig_hash, output_path
        FROM runs;

CREATE TABLE steps_new (
          id              INTEGER PRIMARY KEY AUTOINCREMENT,
          run_id          TEXT NOT NULL,
          name            TEXT NOT NULL,
          started_at      INTEGER NOT NULL,
          finished_at     INTEGER,
          status          TEXT CHECK(status IN ('running','success','failed','skipped')) NOT NULL,
          message         TEXT,
          FOREIGN KEY(run_id) REFERENCES runs_new(run_id)
        );

INSERT INTO steps_new (id, run_id, name, started_at, finished_at, status, message)
        SELECT id, run_id, name, started_at, finished_at, status, message
        FROM steps;

DROP TABLE steps;
DROP TABLE runs;

ALTER TABLE steps_new RENAME TO steps;
ALTER TABLE runs_new RENAME TO runs;