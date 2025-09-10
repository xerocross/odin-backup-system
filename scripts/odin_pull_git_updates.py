#!/usr/bin/env python3

from backuplib.gitpull import git_pull, generate_qsig, get_git_headhash, QuickGitRepoSig
from pathlib import Path
from zoneinfo import ZoneInfo
import datetime
from backuplib.audit import Tracker
from backuplib.logging import setup_logging, WithContext
from backuplib.exceptions import ConfigException
from pydeclarativelib.pydeclarativelib import write_text_atomic
from backuplib.configloader import load_config, OdinConfig
from dataclasses import asdict
from backuplib.backupjob import BackupJobResult
from pydeclarativelib.declarativeaudit import audited_by
from pydeclarativelib.declarativesuccess import with_try_except_and_trace
from typing import List
import json
import uuid
import yaml

def run(parent_id : str):
    return run_git_pull(parent_id=parent_id)

def run_git_pull(parent_id : str = None):
    log = setup_logging(level="INFO", appName="odin_backup_git_pull")
    run_id = "git-pull-"+str(uuid.uuid4())
    log = WithContext(log, {"run_id": run_id})
    if parent_id is not None:
        log = WithContext(log, {"parent_id": parent_id})
    try:

        CONFIG_PATH = Path("~/.config/odin/odin_run_git_pull_job.yaml").expanduser()

        with open(CONFIG_PATH, "r") as f:
            config = yaml.safe_load(f)
        try:
            REPO_DIR = config["REPO_DIR"]
            LOCAL_ZONE = config["LOCAL_ZONE"]
            OUTPUT_PATH = config["OUTPUT_PATH"]
        except KeyError as e:
            raise ConfigException(f"Missing required config key. Config path: {CONFIG_PATH}.")


        tracker = Tracker()
        trace : List[str] = []
        odin_cfg: OdinConfig = load_config()

        local_zone = ZoneInfo(LOCAL_ZONE)
        
        
        log.info(f"starting with run_id {run_id}")
        utc_timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat()


        def write_statefile():
            repo_dir = odin_cfg.repo_dir
            statefile = repo_dir / odin_cfg.git_pull_statefile_name
            state = {
                "hash" : get_git_headhash(repo_path=repo_dir),
                "datetime" : utc_timestamp
            }
            write_text_atomic(at=statefile, the_text=f"{json.dumps(state)}\n")


        @audited_by(tracker, with_step_name="generate qsig", and_run_id = run_id)
        @with_try_except_and_trace(if_success_then_message=None, if_failed_then_message=None, with_trace=trace)
        def generate_qsig_local(from_root : Path):
            qsig: QuickGitRepoSig = generate_qsig(
                            repo_path=odin_cfg.repo_dir
                        )
            qsig_json = json.dumps(asdict(qsig))
            log.debug(f"qsig_json: {qsig_json}")
            log.debug("generated initial qsig", extra = {"repo_path": qsig.repo_path, "head_hash" : qsig.head_hash})
            return {"success" : True,
                    "message": "",
                    "data" : {
                        "qsig_json" : qsig_json
                    }}


        repo_path = odin_cfg.repo_dir
        timestamp = datetime.datetime.now(local_zone).strftime("%Y-%m-%d_%H%M%S")
        
        log.debug("creating tracker")
        tracker.start_run(run_id=run_id,
                      run_name="odin_git_pull",
                      meta={
                            "job": "git pull", 
                            "repo_path": str(repo_path),
                            "timestamp" :  timestamp,
                            "timezone" : LOCAL_ZONE
                        })
    
        if parent_id is not None:
            tracker.set_parent_id(run_id=run_id, parent_id=parent_id)


        generate_qsig_local(from_root=repo_path)

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
        log.info(str(BackupJobResult.SUCCESS))
        result= {
                    "success": True, 
                    "message": "", 
                }
        return result
    except Exception as e:
        log.error(str(BackupJobResult.FAILED))
        log.exception(f"git pull did not finish")
        tracker.finish_run(run_id, "failed", output_path = OUTPUT_PATH)
        raise e

if __name__ == "__main__":
    run(parent_id=None)