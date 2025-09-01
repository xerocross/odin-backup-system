#!/usr/bin/env python3

from backuplib.gitpull import git_pull, generate_qsig, get_git_headhash, QuickGitRepoSig
from pathlib import Path
from zoneinfo import ZoneInfo
import datetime
from backuplib.audit import Tracker
from backuplib.logging import setup_logging, WithContext
from backuplib.exceptions import ConfigException
from backuplib.filesutil import atomic_write_text
from backuplib.configloader import load_config, OdinConfig
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

odin_cfg: OdinConfig = load_config()

local_zone = ZoneInfo(LOCAL_ZONE)
log = setup_logging(level="INFO", appName="odin_backup_git_pull")
run_id = "git-pull-"+str(uuid.uuid4())
log = WithContext(log, {"run_id": run_id})
log.info(f"starting with run_id {run_id}")
utc_timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat()


def run():
    return run_git_pull()

def write_statefile():
    repo_dir = odin_cfg.repo_dir
    statefile = repo_dir / odin_cfg.git_pull_statefile_name
    state = {
        "hash" : get_git_headhash(repo_path=repo_dir),
        "datetime" : utc_timestamp
    }
    atomic_write_text(path=statefile, text=f"{json.dumps(state)}\n")
    pass


def run_git_pull():
    try:
        repo_path = Path(REPO_DIR)
        timestamp = datetime.datetime.now(local_zone).strftime("%Y-%m-%d_%H%M%S")
        tracker = Tracker()
        log.debug("creating tracker")
        tracker.start_run(run_id=run_id,
                      run_name="odin_git_pull",
                      meta={
                            "job": "git pull", 
                            "repo_path": str(repo_path),
                            "timestamp" :  timestamp,
                            "timezone" : LOCAL_ZONE
                        })
    
        with tracker.record_step(run_id =run_id, 
                             name = "generate qsig"
                             ) as rec:
 
            try:
                qsig: QuickGitRepoSig = generate_qsig(
                    repo_path=repo_path
                )
                qsig_json = json.dumps(asdict(qsig))
                log.debug(f"qsig_json: {qsig_json}")
                log.debug("generated initial qsig", extra = {"repo_path": qsig.repo_path, "head_hash" : qsig.head_hash})
                
                rec["status"] = "success"
            except:
                rec["status"] = "failed"
            
            
            
            
        
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
            tracker.finish_run(run_id, "failed")
        
        with tracker.record_step(
                                run_id =run_id, 
                                name = "write statefile"
                             ) as rec:
            write_statefile()


        head_hash = get_git_headhash(repo_path)
        tracker.finish_run(run_id, "success", 
                           output_path= OUTPUT_PATH, 
                           output_sig_hash=head_hash)
        log.info("success")
    except Exception as e:
        log.exception(f"there was an exception {e}")
        tracker.finish_run(run_id, "failed", output_path = OUTPUT_PATH)
        raise e

if __name__ == "__main__":
    try:
        log.info('start', extra={"ODIN_JOB":"git_pull","PHASE":"start"})
        run()
        log.info('done',  extra={"ODIN_JOB":"git_pull","PHASE":"done"})
    except Exception:
        # One journald entry including full traceback:
        log.exception('git pull failed', extra={"ODIN_JOB":"git_pull","PHASE":"error"})
        raise