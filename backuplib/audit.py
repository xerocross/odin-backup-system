#!/usr/bin/env python3
# backuplib/audit.py
from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
import sqlite3, time, json, contextlib
from typing import Any, Optional

DEFAULT_DB = Path.home() / ".odin_backup" / "audit.db"

DDL = """
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
"""

def _now() -> int: return int(time.time())

def _connect(db_path: Path):
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA foreign_keys=ON")
    return conn

def init(db_path: Path = DEFAULT_DB) -> None:
    with _connect(db_path) as c:
        c.executescript(DDL)

@dataclass
class StepRef:
    id: int
    run_id: str
    name: str

class Tracker:
    def __init__(self, db_path: Path = DEFAULT_DB):
        self.db = db_path
        init(self.db)

    # ---- runs ----
    def start_run(self, run_id: str, meta: Optional[dict[str, Any]] = None) -> None:
        with _connect(self.db) as c:
            c.execute(
                "INSERT OR REPLACE INTO runs(run_id, started_at, status, meta_json) VALUES(?,?,?,?)",
                (run_id, _now(), "running", json.dumps(meta or {})),
            )
            c.commit()

    def finish_run(self, run_id: str, status: str) -> None:
        with _connect(self.db) as c:
            c.execute(
                "UPDATE runs SET finished_at=?, status=? WHERE run_id=?",
                (_now(), status, run_id),
            )
            c.commit()

    # ---- steps ----
    def start_step(
        self,
        run_id: str,
        name: str,
        *,
        input_sig: Optional[dict[str, Any]] = None,
        output_path: Optional[Path] = None,
    ) -> StepRef:
        with _connect(self.db) as c:
            cur = c.execute(
                "INSERT INTO steps(run_id,name,started_at,status,input_sig_json,output_path) VALUES(?,?,?,?,?,?)",
                (run_id, name, _now(), "running", json.dumps(input_sig or {}), str(output_path) if output_path else None),
            )
            step_id = cur.lastrowid
            c.commit()
        return StepRef(step_id, run_id, name)

    def finish_step(
        self,
        step: StepRef,
        status: str,
        *,
        message: Optional[str] = None,
        output_sig: Optional[dict[str, Any]] = None,
    ) -> None:
        with _connect(self.db) as c:
            c.execute(
                "UPDATE steps SET finished_at=?, status=?, message=?, output_sig_json=? WHERE id=?",
                (_now(), status, message, json.dumps(output_sig or {}), step.id),
            )
            c.commit()

    # ---- convenience: context manager for steps ----
    @contextlib.contextmanager
    def record_step(
        self,
        run_id: str,
        name: str,
        *,
        input_sig: Optional[dict[str, Any]] = None,
        output_path: Optional[Path] = None,
    ):
        step = self.start_step(run_id, name, input_sig=input_sig, output_path=output_path)
        try:
            yielded = {}
            yield yielded  # caller can stick {"output_sig": {...}} or {"status": "skipped"} in here
            status = yielded.get("status", "success")
            self.finish_step(step, status, output_sig=yielded.get("output_sig"))
        except Exception as e:
            self.finish_step(step, "failed", message=str(e))
            raise

    # ---- tiny reports ----
    def last_runs(self, limit: int = 10) -> list[tuple]:
        with _connect(self.db) as c:
            return list(
                c.execute(
                    "SELECT run_id, datetime(started_at,'unixepoch'), status FROM runs ORDER BY started_at DESC LIMIT ?",
                    (limit,),
                )
            )

    def steps_for(self, run_id: str) -> list[tuple]:
        with _connect(self.db) as c:
            return list(
                c.execute(
                    """SELECT name, status, message,
                              datetime(started_at,'unixepoch'),
                              datetime(finished_at,'unixepoch'),
                              output_path
                       FROM steps WHERE run_id=? ORDER BY id""",
                    (run_id,),
                )
            )
