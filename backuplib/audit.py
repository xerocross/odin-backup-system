
from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
import sqlite3, time, json, contextlib
from typing import Any, Optional
from backuplib.checksumtools import canonicalize_json, sha256_hex
from backuplib.logging import setup_logging, WithContext
from backuplib.runonce import run_once
import functools
from enum import Enum

log = setup_logging(level="INFO", appName="odin_backup_auditing")

DEFAULT_DB = Path.home() / ".odin_backup" / "audit.db"

class AuditDatabaseException(Exception):
    '''There was a problem with the Odin Backup Audit Database'''
    pass

def _now() -> int: return int(time.time())

def _connect(db_path: Path):
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA foreign_keys=ON")
    return conn

def init(db_path: Path = DEFAULT_DB) -> None:
    pass

@dataclass
class StepRef:
    id: int
    run_id: str
    name: str

class RunSignature(Enum):
    CURRENT_UPSTREAM_SIGNATURE = "current_upstream_signature"
    PREVIOUS_UPSTREAM_SIGNATURE = "previous_upstream_signature"
    JOB_RESULT_SIGNATURE = "job_result_signature"
    PREVIOUS_JOB_SIGNATURE = "previous_job_signature"


    def __str__(self):
        return self.value


class Tracker:

    def __init__(self, db_path: Path = DEFAULT_DB):
        self.db = db_path
        self.log = log
        #log = WithContext(log, {"run_log_id": log_run_id})
        init(self.db)

    @run_once
    def start_run(
                    self,
                    run_id: str,
                    run_name: str,
                    input_sig_json: str = None,
                    meta: Optional[dict[str, Any]] = None
                  
                  ) -> None:
        
        canonical_input_sig_json = ""
        input_sig_hash=""
        if input_sig_json is not None:
            canonical_input_sig_json = canonicalize_json(input_sig_json)
            input_sig_hash = sha256_hex(canonical_input_sig_json)
        with _connect(self.db) as c:
            try:
                c.execute(
                    "INSERT OR REPLACE INTO runs(run_id, name, started_at, status, meta_json, input_sig_json, input_sig_hash) VALUES(?,?,?,?,?,?,?)",
                    (run_id, run_name, _now(), "running", json.dumps(meta or {}), canonical_input_sig_json, input_sig_hash),
                )
                c.commit()
            except:
                self.log.exception("could not add audit to start run")
                raise AuditDatabaseException()

    def set_signature_data(self, run_id: str, signature_data: RunSignature, column: RunSignature):
        column_str = str(column)
        with _connect(self.db) as c:
            try:
                c.execute(
                    f"UPDATE runs SET {column_str}=? WHERE run_id=?",
                    (signature_data, run_id),
                )
                c.commit()
            except:
                msg = f"could not add {column_str} to audit db"
                self.log.exception(msg)
                raise AuditDatabaseException(msg)

    @run_once
    def finish_run(self, run_id: str, status: str, *, output_path="", output_sig_hash = "") -> None:
        with _connect(self.db) as c:
            try:
                c.execute(
                    "UPDATE runs SET finished_at=?, status=?, output_path=?, output_sig_hash=? WHERE run_id=?",
                    (_now(), status, output_path, output_sig_hash, run_id),
                )
                c.commit()
            except:
                self.log.exception("could not add audit to finish run")
                raise AuditDatabaseException()

    # ---- steps ----
    def start_step(
        self,
        run_id: str,
        name: str,
    ) -> StepRef:
        with _connect(self.db) as c:
            try:    
                cur = c.execute(
                    "INSERT INTO steps(run_id,name,started_at, status) VALUES(?,?,?,?)",
                    (run_id, name, _now(), "running"),
                )
                step_id = cur.lastrowid
                c.commit()
            except:
                self.log.exception("could not initiate step in audit database")
                raise AuditDatabaseException()
        return StepRef(step_id, run_id, name)

    def finish_step(
        self,
        step: StepRef,
        status: str,
        message: str
    ) -> None:
        with _connect(self.db) as c:
            try:
                c.execute(
                    "UPDATE steps SET finished_at=?, status=?, message=? WHERE id=?",
                    (_now(), status, message, step.id),
                )
                c.commit()
            except:
                self.log.exception("an exception occurred while finishing a step")
                raise AuditDatabaseException()

    # ---- convenience: context manager for steps ----
    @contextlib.contextmanager
    def record_step(
        self,
        run_id: str,
        name: str,
    ):
        step = self.start_step(run_id, name)
        try:
            yielded = {}
            yield yielded 
            status = yielded.get("status", "success")
            message = yielded.get("message", "")
            self.finish_step(step, status, message=message)
        except Exception as e:
            self.finish_step(step, "failed", message=str(e))
            raise e
        
        
    def audit_this(self, run_id, name):
        """
        Decorator factory: takes run_id and step name,
        returns a decorator that wraps the function to record it.
        """
        def decorator(func):
            tracker = self
            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                # create the step in DB
                step = tracker.start_step(run_id, name)
                rec = {}  # the "audit handle" dict
                try:
                    # call the original function, injecting `rec`
                    result = func(tracker, rec, *args, **kwargs)
                    status = rec.get("status", "success")
                    message = rec.get("message", "")
                    tracker.finish_step(step, status, message)
                    return result
                except Exception as e:
                    tracker.finish_step(step, "failed", message=str(e))
                    raise
            return wrapper
        return decorator
        

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
            

            
