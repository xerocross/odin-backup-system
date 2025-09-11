from pathlib import Path
from typing import List
from backuplib.logging import setup_logging, Logger, WithContext
from backuplib.configloader import OdinConfig, load_config
from backuplib.checksumtools import write_sha256_sidecar
from backuplib.gpgtools import gpg_sign_detached
from backuplib.audit import Tracker, RunSignature
from backuplib.jobstatehelper import get_hash, get_upstream_hash
from pydeclarativelib.pydeclarativelib import make_a_tarball, write_text_atomic
from pydeclarativelib.declarativeaudit import audited_by
from pydeclarativelib.declarativesuccess import with_try_except_and_trace
from dataclasses import dataclass, field
from backuplib.backupjob import BackupJobResult
import datetime
from zoneinfo import ZoneInfo
import shutil, os
import json
import uuid

odin_cfg: OdinConfig = load_config()
logger : Logger = setup_logging(appName = "odin-gen-tarball-backup")
tz = ZoneInfo(odin_cfg.local_zone)
datestamp = datetime.datetime.now(tz).strftime("%Y_%m_%d-%H-%M")
utc_timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat()
run_id = "odin-tarball-"+str(uuid.uuid4())
logger = WithContext(logger, {"run_id": run_id})


tracker = Tracker()
trace : List[str] = []




@audited_by(tracker, with_step_name="make idempotent copy", and_run_id = run_id)
@with_try_except_and_trace(if_success_then_message=None, if_failed_then_message=None, with_trace=trace)
def make_idempotent_copy(
                    new_tarball_file_path: Path, 
                    tarball_dir_idempotent : Path):
    shutil.copy(new_tarball_file_path, tarball_dir_idempotent)
    return {"success": True, "message" :  None}


@audited_by(tracker, with_step_name="generate tarball", and_run_id = run_id)
@with_try_except_and_trace(if_success_then_message="generated tarball", if_failed_then_message="could not generate the tarball", with_trace=trace)
def create_tarball( 
                repo_dir: Path, 
                exclude_patterns: list[str], 
                dest_tar_gz: Path) -> None:
    make_a_tarball(of_dir= repo_dir, at=dest_tar_gz, excluding=exclude_patterns)
    return {"success": True}


@audited_by(tracker, with_step_name="write state file", and_run_id = run_id)
@with_try_except_and_trace(if_success_then_message=None, if_failed_then_message=None, with_trace=trace)
def write_state(
                marker_file : Path,
                latest_tarball : str
                ):
    write_text_atomic(at=marker_file, the_text = latest_tarball + "\n")
    return {"success": True}

@dataclass
class IdempotentStateData:
    job_hash : str
    datetime: str

@audited_by(tracker, with_step_name="write idempotent state", and_run_id = run_id)
@with_try_except_and_trace(if_success_then_message=None, if_failed_then_message=None, with_trace=trace)
def write_idempotent_state(
                                tarball_idempotent_path : Path,
                                upstream_hash : str
                           ):
    _, hash = write_sha256_sidecar(tarball_idempotent_path)
    idempotent_tar_path = odin_cfg.tarball_dir_idempotent
    state_filename = odin_cfg.tarball_state_filename
    statefile_path = idempotent_tar_path / state_filename
    state = {
        "datetime" : utc_timestamp,
        "hash" : hash,
        "upstream_hash": upstream_hash
    }
    data: IdempotentStateData = IdempotentStateData(job_hash=hash, datetime=utc_timestamp)
    logger.info(f"generated idempotent odin backup with datetime {utc_timestamp} and sha245 {hash}")
    write_text_atomic(at=statefile_path, the_text=f"{json.dumps(state)}\n")
    msg = f"generated Odin tarball state file at {statefile_path}"
    logger.info(msg)
    return {"success": True, "message" : msg, "data" : data} 




@audited_by(tracker, with_step_name="sign idempotent copy", and_run_id = run_id)
@with_try_except_and_trace(if_success_then_message=None, if_failed_then_message=None, with_trace=trace)
def gpg_sign(
                            the_item: Path,
                            *,
                            by_signer: str | None = None,      # key id / fingerprint / uid to sign with
                        ):
        sig_file = gpg_sign_detached(
            artifact = the_item,
            signer = by_signer,
            armor = False,
        )
        return {"success": True, "sig_path": sig_file} 

@dataclass
class StateCheckData:
    upstream_hash : str
    last_recorded_upstream_hash : str


@audited_by(tracker, with_step_name="check for state change", and_run_id = run_id)
@with_try_except_and_trace(if_success_then_message=None, if_failed_then_message=None, with_trace=trace)
def check_for_state_change(
                    statepath : Path,
                    upstream_statepath: Path,
                            ):
    upstream_hash = "A"
    last_recorded_upstream_hash = "B"
    if upstream_statepath.exists():
        upstream_hash = get_hash(statefile_path=upstream_statepath)
    if statepath.exists():
        last_recorded_upstream_hash = get_upstream_hash(statefile_path=statepath)
    logger.info(f"upstream_hash: {upstream_hash}; last_recorded_upstream_hash: {last_recorded_upstream_hash}")
    data = StateCheckData(upstream_hash= upstream_hash,
                          last_recorded_upstream_hash=last_recorded_upstream_hash
                          )
    if upstream_hash == last_recorded_upstream_hash:
        
        result = {"success": True, "message": "no upstream change: skipping","upstream_hash_has_not_changed" : True}
        
    result= {
                "success": True, 
                "message": "found upstream changes", 
                "upstream_hash_has_not_changed" : False,
            }
    return {**result, "data" : data}


def run(parent_id: str = None):
    return main(parent_id=parent_id)

def main(parent_id : str = None):
    try:
        do_regardless = False

        tracker.start_run(run_id=run_id,
                run_name="odin_tarball",
                meta={
                    "timestamp" :  utc_timestamp,
                    "timezone" : odin_cfg.local_zone
                })
        
        if parent_id is not None:
            tracker.set_parent_id(run_id=run_id, parent_id=parent_id)

        ## setup paths
        statepath = odin_cfg.tarball_dir_idempotent / odin_cfg.tarball_state_filename
        upstream_statepath = odin_cfg.tarball_job_upstream_statepath
        previous_job_hash = get_hash(statepath)

        state_chage_results = check_for_state_change(
                statepath = statepath,
                upstream_statepath = upstream_statepath,
        )
        do_skip = state_chage_results["upstream_hash_has_not_changed"]
        state_check_data : StateCheckData = state_chage_results["data"]
        upstream_hash = state_check_data.upstream_hash
        last_recorded_upstream_hash = state_check_data.last_recorded_upstream_hash

        tracker.set_signature_data(run_id = run_id,
                                   signature_data = previous_job_hash, 
                                   column = RunSignature.PREVIOUS_JOB_SIGNATURE)

        tracker.set_signature_data(run_id = run_id,
                                   signature_data = upstream_hash, 
                                   column = RunSignature.CURRENT_UPSTREAM_SIGNATURE)

        tracker.set_signature_data(run_id = run_id,
                           signature_data = last_recorded_upstream_hash, 
                           column = RunSignature.PREVIOUS_UPSTREAM_SIGNATURE)

        if do_skip and not do_regardless:
            logger.info("skipping because upstream hash has not changed")
            tracker.finish_run(run_id, "skipped")
            return {"success": True}
        
        
        repo_dir = odin_cfg.repo_dir
        exclude_list = odin_cfg.tarball_exclusions
        tarball_location = odin_cfg.tarball_dir
        tarball_filename = f"Odin_{datestamp}.tar.gz"
        tarball_idempotent_filename = odin_cfg.default_tarball_name
        tarball_path = tarball_location / tarball_filename
        marker_file = tarball_location / "latest.txt"
        tarball_idempotent_path = odin_cfg.tarball_dir_idempotent / tarball_idempotent_filename


        create_tarball(
                            repo_dir=repo_dir,
                            exclude_patterns=exclude_list,
                            dest_tar_gz=tarball_path
                        )
        
        write_state(
                        marker_file=marker_file, 
                        latest_tarball = str(tarball_path),
                    )
        make_idempotent_copy(
                                new_tarball_file_path=tarball_path, 
                                tarball_dir_idempotent= tarball_idempotent_path
                            )
    
        idempotent_state_res = write_idempotent_state(
                                tarball_idempotent_path = tarball_idempotent_path,
                                upstream_hash = upstream_hash
                              )
        idempotent_state_data : IdempotentStateData = idempotent_state_res["data"]

        tracker.set_signature_data(run_id = run_id,
                           signature_data = idempotent_state_data.job_hash, 
                           column = RunSignature.JOB_RESULT_SIGNATURE)
        
        gpg_sign(the_item= tarball_idempotent_path,by_signer=odin_cfg.recipient)


        logger.info(str(BackupJobResult.SUCCESS))
        tracker.finish_run(run_id, "success", output_path = str(odin_cfg.tarball_dir_idempotent))
        return {"success": True}
    except Exception as e:
        tracker.finish_run(run_id, "failed")
        logger.error(str(BackupJobResult.FAILED))
        trace = [t for t in trace if t is not None]
        logger.error("trace: [" + ", ".join(trace) + "]\n")
        logger.exception("did not generate the Odin backup tarball successfully")
        raise e
    
if __name__ == "__main__":
    main(parent_id=None)
