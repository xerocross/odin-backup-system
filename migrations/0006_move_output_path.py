# 0006_move_output_path.py
# Drop steps.output_path, add runs.output_path

import sqlite3

def migrate(conn: sqlite3.Connection):
    conn.execute("PRAGMA foreign_keys=OFF")

    # --- Rebuild steps without output_path ---
    conn.execute("""
        CREATE TABLE steps_new (
          id              INTEGER PRIMARY KEY AUTOINCREMENT,
          run_id          TEXT NOT NULL,
          name            TEXT NOT NULL,
          started_at      INTEGER NOT NULL,
          finished_at     INTEGER,
          status          TEXT CHECK(status IN ('running','success','failed','skipped')) NOT NULL,
          message         TEXT,
          FOREIGN KEY(run_id) REFERENCES runs(run_id)
        )
    """)

    # Copy data from old table (exclude dropped column)
    conn.execute("""
        INSERT INTO steps_new (id, run_id, name, started_at, finished_at, status, message)
        SELECT id, run_id, name, started_at, finished_at, status, message
        FROM steps
    """)

    conn.execute("DROP TABLE steps")
    conn.execute("ALTER TABLE steps_new RENAME TO steps")

    # Recreate index
    conn.execute("CREATE INDEX IF NOT EXISTS idx_steps_run ON steps(run_id)")

    # --- Add output_path to runs ---
    cols = [r[1] for r in conn.execute("PRAGMA table_info(runs)")]
    if "output_path" not in cols:
        conn.execute("ALTER TABLE runs ADD COLUMN output_path TEXT")

    conn.execute("PRAGMA foreign_keys=ON")
