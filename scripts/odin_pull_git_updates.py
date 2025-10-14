#!/usr/bin/env python3

from backuplib.gitpull import git_pull, generate_qsig, get_git_headhash, QuickGitRepoSig
from pathlib import Path
from zoneinfo import ZoneInfo
import datetime
from backuplib.audit import Tracker
from backuplib.logging import setup_logging, WithContext
from pydeclarativelib.pydeclarativelib import write_text_atomic
from backuplib.configloader import load_config, OdinConfig
from dataclasses import asdict
from backuplib.backupjob import BackupJobResult
from pydeclarativelib.declarativeaudit import audited_by
from typing import Dict, Any
from localtypes.projecttypes import JobStageInfo
import json
import uuid

def run(parent_id : str | None)-> JobStageInfo:
    return run_git_pull(parent_id=parent_id)

def run_git_pull(parent_id : str | None = None)->JobStageInfo:
    log = setup_logging(level="INFO", appName="odin_backup_git_pull")
    run_id = "git-pull-"+str(uuid.uuid4())
    log = WithContext(log, {"run_id": run_id})
    if parent_id is not None:
        log = WithContext(log, {"parent_id": parent_id})
    tracker = Tracker()
    odin_cfg: OdinConfig = load_config()
    try:
        local_zone = ZoneInfo(odin_cfg.local_zone)
        repo_path = odin_cfg.repo_dir
        timestamp = datetime.datetime.now(local_zone).strftime("%Y-%m-%d_%H%M%S")
        log.info(f"starting with run_id {run_id}")
        utc_timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat()

        @audited_by(tracker, with_step_name="write statefile", and_run_id = run_id)
        def write_statefile() -> JobStageInfo:
            repo_dir = odin_cfg.repo_dir
            statefile = repo_dir / odin_cfg.git_pull_statefile_name
            state = {
                "hash" : get_git_headhash(repo_path=repo_dir),
                "datetime" : utc_timestamp
            }
            write_text_atomic(at=statefile, the_text=f"{json.dumps(state)}\n")
            return {
                "success": True
            }


        @audited_by(tracker, with_step_name="generate qsig", and_run_id = run_id)
        def generate_qsig_local(from_root : Path) -> JobStageInfo:
            qsig: QuickGitRepoSig = generate_qsig(
                            repo_path=odin_cfg.repo_dir
                        )
            qsig_json = json.dumps(asdict(qsig))
            log.debug(f"qsig_json: {qsig_json}")
            log.debug("generated initial qsig", extra = {"repo_path": qsig.repo_path, "head_hash" : qsig.head_hash})
            return {"success" : True,
                    "data" : {
                        "qsig_json" : qsig_json
                    }}


        tracker.start_run(run_id=run_id,
                      run_name="odin_git_pull",
                      meta={
                            "job": "git pull", 
                            "repo_path": str(repo_path),
                            "timestamp" :  timestamp,
                            "timezone" : odin_cfg.local_zone
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
            log_message_dict : Dict[str, Any] = {
                "message" : f"the git_pull method exited with problematic code {return_code}",
                "rout": rout,
                "rerr": rerr,
                "summary": summary
            }
            log.error(json.dumps(log_message_dict))
            tracker.finish_run(run_id, "failed")
        

        write_statefile()
        head_hash = get_git_headhash(repo_path)
        tracker.finish_run(run_id, "success", 
                           output_path= odin_cfg.repo_dir.as_posix(), 
                           output_sig_hash=head_hash)
        log.info(str(BackupJobResult.SUCCESS))
        return {
                    "success": True,
                }
    except Exception as e:
        log.error(str(BackupJobResult.FAILED))
        log.exception(f"git pull did not finish")
        tracker.finish_run(run_id, "failed")
        raise e

if __name__ == "__main__":
    run(parent_id=None)