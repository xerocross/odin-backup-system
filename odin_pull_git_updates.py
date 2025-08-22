#!/usr/bin/env python3

from backuplib.gitpull import git_pull
from pathlib import Path
from zoneinfo import ZoneInfo
from datetime import datetime
from backuplib.audit import Tracker
import uuid


local_zone = ZoneInfo("America/New_York")

REPO_DIR="/home/adam/OdinBack2"

def run_git_pull():
    repo_path = Path(REPO_DIR)
    timestamp = datetime.now(local_zone).strftime("%Y-%m-%d_%H%M%S")
    tracker = Tracker()
    run_id = uid = uuid.uuid4();
    tracker.start_run(run_id, meta={"job": "git pull", 
                                    "repo_path": str(repo_path),
                                    "timestamp" :  timestamp
                                    })
    
    with tracker.record_step(run_id, "perform git pull", input_sig=qsig, output_path=manifest_path) as rec:
        return_code, rout, rerr, summary = git_pull(
            repo_path = repo_path,
            remote = "origin",
            branch= "main",
            rebase = False,
            ff_only = True,
            timeout = 3000
        )