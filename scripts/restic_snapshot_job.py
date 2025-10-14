#!/usr/bin/env python3
from __future__ import annotations
import uuid
from backuplib.logging import setup_logging, WithContext
from backuplib.audit import Tracker
from backuplib.configloader import OdinConfig, load_config
from pydeclarativelib.declarativeaudit import audited_by
from pathlib import Path
from localtypes.projecttypes import JobStageInfo
import datetime
import subprocess

def run(parent_id : str | None = None):
    logger = setup_logging(appName="odin-restic-snapshot")
    tracker = Tracker()
    run_id = "odin-restic-snapshot-"+str(uuid.uuid4())
    logger = WithContext(logger, {"run_id": run_id}) # type: ignore
    if parent_id is not None:
            logger = WithContext(logger, {"parent_id": parent_id})# type: ignore
    odin_config: OdinConfig = load_config()
    utc_timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat()


    @audited_by(tracker, with_step_name="create_restic_snapshot", and_run_id = run_id)
    def run_restic_snapshot(
                            repo_path: Path,
                            restic_repo_path: Path, 
                            excludes_file_path: Path,
                            password_file_path: Path) -> JobStageInfo:
        cmd = ["restic backup", 
            "--one-file-system",
            "--exclude-file", 
            excludes_file_path.as_posix(),
            "--tag", 
            "odin",
            "-r", 
            restic_repo_path.as_posix(),
            "-p", 
            password_file_path.as_posix(),
            repo_path.as_posix()]
        try:
            with subprocess.Popen(
                    cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,  # capture stderr too 
                    text=True,
                    bufsize=1,
                ) as p:
                    log_msg = ""
                    if p.stdout is not None:
                        for line in p.stdout:
                            log_msg = log_msg + line.rstrip() + "; "
                    if p.stderr is not None:
                        for line in p.stderr:
                            log_msg = log_msg + line.rstrip() + "; "
                    logger.info("restic backup/snapshot log:" + log_msg)
                    rc = p.wait()
                    if rc != 0:
                        raise subprocess.CalledProcessError(rc, cmd)
                    return {"success": True, "data" :  None}
        except subprocess.CalledProcessError as e:
            logger.exception(f"Subprocess error: {e.stderr.strip()}")
            return {"success": False, "data" :  None}
        except subprocess.TimeoutExpired:
            logger.exception(f"Subprocess timeout")
            return {"success": False, "data" :  None}

    tracker.start_run(run_id=run_id,
                run_name="odin_restic_snapshot_job",
                meta={
                    "timestamp" :  utc_timestamp,
                    "timezone" : odin_config.local_zone
                })
    

    run_restic_snapshot(
                            repo_path=odin_config.repo_dir,
                            restic_repo_path=odin_config.restic_snapshot_job.restic_repo_path,
                            excludes_file_path=odin_config.restic_snapshot_job.restic_excludes_file_path,
                            password_file_path=odin_config.restic_snapshot_job.restic_password_file_path
                        )

if __name__ == "__main__":
    run(parent_id=None)