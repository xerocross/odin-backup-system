# 0005_add_runs_name.py
# Adds runs.name and backfills from meta_json.job using Python (no JSON1 needed).

import json
import sqlite3

def migrate(conn: sqlite3.Connection):
    conn.execute("PRAGMA foreign_keys=ON")

    # 1) Ensure column exists (idempotent)
    cols = [r[1] for r in conn.execute("PRAGMA table_info(runs)")]
    if "name" not in cols:
        conn.execute("ALTER TABLE runs ADD COLUMN name TEXT")

    # 2) Backfill from meta_json.job, ignoring malformed JSON
    cur = conn.execute("""
        SELECT rowid, meta_json
        FROM runs
        WHERE name IS NULL AND meta_json IS NOT NULL
    """)

    updates = []
    for rowid, mj in cur:
        if mj is None:
            continue
        # Defensive decode if the DB ever stores bytes
        if not isinstance(mj, str):
            try:
                mj = mj.decode("utf-8", errors="ignore")
            except Exception:
                continue
        try:
            obj = json.loads(mj)
        except Exception:
            continue
        job = obj.get("job")
        if isinstance(job, str):
            job = job.strip()
            if job:
                updates.append((job, rowid))

    if updates:
        conn.executemany("UPDATE runs SET name = ? WHERE rowid = ?", updates)
