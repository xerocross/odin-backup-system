#!/usr/bin/env python3

from backuplib.gitpull import git_pull, generate_qsig, QuickGitRepoSig
from pathlib import Path
from zoneinfo import ZoneInfo
from datetime import datetime
from backuplib.audit import Tracker
from backuplib.logging import setup_logging, WithContext
from dataclasses import asdict
import json
import uuid


local_zone = ZoneInfo("America/New_York")
REPO_DIR="/home/adam/OdinBack2"

log = setup_logging(level="INFO", appName="odin_backup_git_pull")  # respects LOG_LEVEL, SERVICE_NAME
run_id = str(uuid.uuid4())
log_run_id = str(run_id)[:12]
log = WithContext(log, {"run_log_id": log_run_id})
log.info(f"starting with run_id {run_id}")
def run_git_pull():
    repo_path = Path(REPO_DIR)
    timestamp = datetime.now(local_zone).strftime("%Y-%m-%d_%H%M%S")
    tracker = Tracker()
    
    qsig: QuickGitRepoSig = generate_qsig(
        repo_path=repo_path
    )
    qsig_json = json.dumps(asdict(qsig))
    log.info("generated initial qsig", extra = {"repo_path": qsig.repo_path, "head_hash" : qsig.head_hash})
    log.info("creating tracker")

    tracker.start_run(run_id=run_id,
                      run_name="odin_git_pull",
                      input_sig_json = qsig_json,
                      meta={"job": "git pull", 
                                    "repo_path": str(repo_path),
                                    "timestamp" :  timestamp,
                        })
    try:
        return_code, rout, rerr, summary = git_pull(
            repo_path = repo_path,
            remote = "origin",
            branch= "main",
            rebase = False,
            ff_only = True,
            timeout = 30000,
            tracker=tracker,
            run_id = run_id
        )
        if (return_code != 0):
            log.error(f"the git_pull method exited with problematic code {return_code}")
        tracker.finish_run(run_id, "success")
    except Exception as e:
        log.exception(f"there was an exception {e}")
        tracker.finish_run(run_id, "failed", output_path = REPO_DIR)

if __name__ == "__main__":
    run_git_pull()