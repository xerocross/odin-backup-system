#!/usr/bin/env python3

from backuplib.gitpull import git_pull, generate_qsig, get_git_headhash, QuickGitRepoSig
from pathlib import Path
from zoneinfo import ZoneInfo
from datetime import datetime
from backuplib.audit import Tracker
from backuplib.logging import setup_logging, WithContext
from backuplib.exceptions import ConfigException
from dataclasses import asdict
import json
import uuid
import yaml

CONFIG_PATH = Path("~/.config/odin/odin_run_git_pull_job.yaml").expanduser()
with open(CONFIG_PATH, "r") as f:
    config = yaml.safe_load(f)
try:
    REPO_DIR = config["REPO_DIR"]
    LOCAL_ZONE = config["LOCAL_ZONE"]
    OUTPUT_PATH = config["OUTPUT_PATH"]
except KeyError as e:
    raise ConfigException(f"Missing required config key. Config path: {CONFIG_PATH}.")


local_zone = ZoneInfo(LOCAL_ZONE)
log = setup_logging(level="INFO", appName="odin_backup_git_pull")
run_id = "git-pull"+str(uuid.uuid4())
log_run_id = run_id
log = WithContext(log, {"run_log_id": log_run_id})
log.info(f"starting with run_id {run_id}")

def run():
    return run_git_pull()

def run_git_pull():
    repo_path = Path(REPO_DIR)
    timestamp = datetime.now(local_zone).strftime("%Y-%m-%d_%H%M%S")
    tracker = Tracker()
    
    qsig: QuickGitRepoSig = generate_qsig(
        repo_path=repo_path
    )
    qsig_json = json.dumps(asdict(qsig))
    log.debug("generated initial qsig", extra = {"repo_path": qsig.repo_path, "head_hash" : qsig.head_hash})
    log.info("creating tracker")

    tracker.start_run(run_id=run_id,
                      run_name="odin_git_pull",
                      input_sig_json = qsig_json,
                      meta={
                            "job": "git pull", 
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
        head_hash = get_git_headhash(repo_path)
        tracker.finish_run(run_id, "success", 
                           output_path= OUTPUT_PATH, 
                           output_sig_hash=head_hash)
    except Exception as e:
        log.exception(f"there was an exception {e}")
        tracker.finish_run(run_id, "failed", output_path = OUTPUT_PATH)

if __name__ == "__main__":
    run_git_pull()