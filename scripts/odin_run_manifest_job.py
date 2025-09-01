from __future__ import annotations
from pathlib import Path
import os, tempfile
from backuplib.generate_manifest import write_manifest
from backuplib.audit import Tracker
from backuplib.filesutil import quick_scan_signature, atomic_write_text, QuickManifestSig
from backuplib.checksumtools import compute_sha256, digest
from backuplib.exceptions import ConfigException
from backuplib.configloader import OdinConfig, load_config
from backuplib.logging import setup_logging, WithContext
import json
from zoneinfo import ZoneInfo
from datetime import datetime
from dataclasses import dataclass
from typing import Dict, List
from dataclasses import asdict
import logging
import uuid
import yaml
from logging import Logger
import contextvars
from enum import Enum
current_tracker = contextvars.ContextVar("current_tracker", default=None)
current_run_id  = contextvars.ContextVar("current_run_id",  default=None)


global_log = setup_logging(level="INFO", appName="odin_generate_manifest") 
# Load a YAML file into a Python dict
CONFIG_PATH = Path("~/.config/odin/odin_run_manifest_job.yaml").expanduser()




with open(CONFIG_PATH, "r") as f:
    config = yaml.safe_load(f)

try:
    REPO_DIR = Path(config["REPO_DIR"])
    ODIN_MANIFEST_DIR = Path(config["ODIN_MANIFEST_DIR"])
    EXCLUSIONS = config["EXCLUSIONS"]
    OUTPUT_PATH = config["OUTPUT_PATH"]
    LOCAL_ZONE = config["LOCAL_ZONE"]
    MANIFEST_STATE_NAME = config["MANIFEST_STATE_NAME"]
except KeyError as e:
    raise ConfigException(f"Missing required config key. Config path: {CONFIG_PATH}.")

state_path = ODIN_MANIFEST_DIR / MANIFEST_STATE_NAME

config_dict = {
    "REPO_DIR": REPO_DIR,
    "ODIN_MANIFEST_DIR": ODIN_MANIFEST_DIR,
    "MANIFEST_EXCLUSIONS": EXCLUSIONS,
    "MANIFEST_OUTPUT_PATH": OUTPUT_PATH,
    "LOCAL_ZONE": LOCAL_ZONE,
    "MANIFEST_STATE_NAME" : MANIFEST_STATE_NAME,
    "STATE_PATH": state_path
}
config_fingerprint = digest(config_dict)
script_path = Path(__file__)
local_zone = ZoneInfo(LOCAL_ZONE)

odinConfig: OdinConfig = load_config()

#
# class run_context:
#     def __init__(self, tracker, run_id):
#         self.tok_t = self.tok_r = None
#         self.tracker, self.run_id = tracker, run_id
#     def __enter__(self):
#         self.tok_t = current_tracker.set(self.tracker)
#         self.tok_r = current_run_id.set(self.run_id)
#     def __exit__(self, *exc):
#         current_run_id.reset(self.tok_r)
#         current_tracker.reset(self.tok_t)


@dataclass
class ManifestInfo:
    run_id : str
    repo_dir: str
    init_qsig : QuickManifestSig | None
    initial_signature_hash: str | None
    output_signature_hash: str | None
    timestamp: str | None
    state_path: Path | None
    manifest_path : Path | None
    current_manifest_sig : str | None

def localtimestamp() -> str:
    return datetime.now(local_zone).strftime("%Y-%m-%d_%H%M%S")

def get_manifest_state():
    with open(config_dict["STATE_PATH"], 'r', encoding="utf-8") as f:
        data = json.load(f)
        return data



def write_manifest_state_file(
                                run_id: str,
                                tracker: Tracker,
                                odinConfig: OdinConfig,
                                initial_quick_sig : dict,
                                manifestInfo: ManifestInfo,
                                logger : Logger
                            ):
    with tracker.record_step(run_id, "generating manifest state file") as rec:
        try:
            state_path = odinConfig.manifest_dir / odinConfig.manifest_state_name
            manifest_path = odinConfig.manifest_dir / odinConfig.manifest_file_name
            out_sha = compute_sha256(manifest_path)
            state_doc = ManifestInfo(
                
                root_path=odinConfig.repo_dir,
                init_qsig=initial_quick_sig,
                init_sig_hex= qsig_hex,
                output_sig_hex=out_sha,
                timestamp=datetime.now(local_zone).strftime("%Y-%m-%d_%H:%M:%S")
            )
            atomic_write_text(state_path, json.dumps(asdict(state_doc), sort_keys=True, indent=2))
            rec["status"] = "success"
            logger.info(f"odin manifest job completed successfully; output at {OUTPUT_PATH}")
            tracker.finish_run(run_id, "success", 
                        output_path = OUTPUT_PATH,
                        output_sig_hash = state_doc.output_sig_hex)
        except Exception as e:
            rec["failed"]
            logger.exception(f"{e}")
            tracker.finish_run(run_id, "failed", output_path = OUTPUT_PATH)
            raise e
    pass


def run():
    odin_run_manifest_job()



def perform_initial_job_setup(odinConfig: OdinConfig, *, 
                              run_id : str, 
                              logger : Logger) -> ManifestInfo:
    
    logger.info(f"starting the odin manifest job run_id: {run_id}")
    out_dir = Path(ODIN_MANIFEST_DIR)
    out_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = odinConfig.manifest_dir / odinConfig.manifest_file_name
    state_path = odinConfig.manifest_dir / odinConfig.manifest_state_name
    logger.debug(f"found state path: {state_path}")
    quick_signature : QuickManifestSig = quick_scan_signature(
                                            root = odinConfig.repo_dir, 
                                            exclude=odinConfig.manifest_exclusions
                                        )
    initial_signature_hash = digest(asdict(quick_signature))
    logger.info(f"computed initial quick-signature hash: {initial_signature_hash}")
    current_manifest_sig = compute_sha256(manifest_path)
    manifestInfo = ManifestInfo(
        run_id = run_id,
        repo_dir = odinConfig.repo_dir,
        init_qsig = quick_signature,
        initial_signature_hash = initial_signature_hash,
        manifest_path = manifest_path,
        state_path = state_path,
        current_manifest_sig = current_manifest_sig
    )
    return manifestInfo


def get_manifest_path(odinConfig : OdinConfig) -> Path:
    return odinConfig.manifest_dir / odinConfig.manifest_file_name

def get_state_path(odinConfig : OdinConfig) -> Path:
    return odinConfig.manifest_dir / odinConfig.manifest_state_name


class JobState(Enum):
    STATE_EXPIRED = 1
    STATE_NOT_FOUND_SHOULD_UPDATE = 2
    STILL_CURRENT_SHOULD_SKIP = 3
    STATE_CHECK_FAILED_SHOULD_REBUILD = 4

@dataclass
class ManifestState():
    init_signature: str
    output_signature_hash: str


def load_state(
                    manifestInfo: ManifestInfo,
                    tracker : Tracker,
                    logger : Logger
                ) -> ManifestState:
    state_path = manifestInfo.state_path
    state = json.loads(state_path.read_text(encoding="utf-8"))
    state = ManifestState(
        init_signature = state.get("init_sig_hex"),
        output_signature_hash = state.get("output_sig_hex")
        )
    return state

def decide_whether_to_do_this_job(
                                    *,
                                    odinConfig : OdinConfig,
                                    tracker : Tracker,
                                    run_id : str,
                                    manifestInfo: ManifestInfo,
                                    logger : Logger,
                                    manifestState : ManifestState
                                    ) -> JobState:
    with tracker.record_step(
                            run_id=run_id, 
                            name="check signature"
                            ) as rec:
        
        manifest_path = get_manifest_path(odinConfig)
        manifestInfo.manifest_path = manifest_path
        
        if manifest_path.exists() and state_path.exists():
            logger.debug("found that manifest and manifest state exist")
            try:
                state_init_sig_hex = manifestState.init_signature
                logger.info(f"found that state init siq hex is {state_init_sig_hex}")
                
                if state_init_sig_hex == manifestInfo.initial_signature_hash:
                    # Optional extra safety: verify the manifest hash still matches
                    previous_state_sig = manifestState.output_signature_hash
                    current_state_sig = manifestInfo.current_manifest_sig
                    
                    logger.info(f"found previous state sig {previous_state_sig} and current state sig {current_state_sig}")
                    if previous_state_sig == manifestInfo.current_manifest_sig:
                        msg = "found existing state matches current: skipping"
                        logger.info("found existing state matches current: skipping")
                        rec["status"] = "success"
                        rec["message"] = msg
                        return JobState.STILL_CURRENT_SHOULD_SKIP
                    else:
                        msg = "current state sig is new"
                        logger.info(msg)
                        rec["status"] = "success"
                        rec["message"] = msg
                        return JobState.STATE_EXPIRED
                        # write_manifest_helper(
                        #     manifest_path,
                        #     tracker = tracker,
                        #     log = logger,
                        #     manifestInfo= manifestInfo,
                        # )
                else:
                    msg = "state info not found"
                    logger.info("state info not found")
                    rec["status"] = "success"
                    rec["message"] = msg
                    return JobState.STATE_NOT_FOUND_SHOULD_UPDATE
                    # write_manifest_helper(
                    #     manifest_path,
                    #     tracker = tracker,
                    #     run_id = run_id,
                    #     log = logger,
                    #     initial_quick_sig=qsig,
                    #     qsig_hex=qsig_hex,
                    # )
            except Exception as e:
                rec["status"] = "failed"
                msg = "encountered an exception while checking state: falling back on re-build"
                rec["message"] = msg
                logger.exception(msg)
                return JobState.STATE_CHECK_FAILED_SHOULD_REBUILD
                #
                # write_manifest_helper(
                #     manifest_path,
                #     tracker = tracker,
                #     run_id = run_id,
                #     log = log,
                #     initial_quick_sig=qsig,
                #     qsig_hex=qsig_hex,
                # )
        else:
            
            msg = "found no existing manifest or state"
            logger.info(msg)
            return JobState.STATE_NOT_FOUND_SHOULD_UPDATE
            # write_manifest_helper(
            #     manifest_path,
            #     tracker = tracker,
            #     run_id = run_id,
            #     initial_quick_sig=qsig,
            #     qsig_hex=qsig_hex,
            #     log = log
            # )



def odin_run_manifest_job():
    run_id = "manifest-" + str(uuid.uuid4())
    logger = WithContext(global_log, {"log_id": run_id})
    
    

    
    
    try:
        tracker = Tracker()
        tracker.start_run(run_id = run_id,
                            run_name="generate_manifest",
                            meta={
                                "job": "generate manifest", 
                            })
        
        
        manifestInfo = perform_initial_job_setup(
                                        odinConfig, 
                                        run_id = run_id, 
                                        logger = logger
                                      )
        
        state_path = get_state_path(odinConfig)
        manifestInfo.state_path = state_path
        
        manifestState : ManifestState = load_state(
                                        manifestInfo = manifestInfo,
                                        tracker = tracker,
                                        logger= logger
                                        )
    
    
        
        
        
        job_state : JobState = decide_whether_to_do_this_job(   
                                    odinConfig=odinConfig,
                                    tracker=tracker,
                                    run_id=run_id ,
                                    manifestInfo=manifestInfo,
                                    logger=logger,
                                    manifestState = manifestState
                                )
        
        
        
        
        
        
    except Exception as e:
        logger.exception(f"{e}")
    


def generate_odin_manifest():
    pass


def write_manifest_helper(manifest_path,
                          *,
                          tracker : Tracker,
                          run_id: str,
                          initial_quick_sig: QuickManifestSig,
                          qsig_hex: str,
                          logger : Logger):

    with tempfile.TemporaryDirectory() as tmpdir:
        logger.debug(f"working in temporary directory {tmpdir}")
        
        with tracker.record_step(run_id, "generating odin manifest") as rec:
            
            
            manifest_temp_file_path = os.path.join(tmpdir, "manifest.yaml")
            logger.info("starting to generate manifest")
            try:
                write_manifest(
                            root_dir=REPO_DIR, 
                            manifest_path = manifest_temp_file_path, 
                            format_type="yaml", 
                            exclude_patterns=EXCLUSIONS
                        )
                os.replace(manifest_temp_file_path, manifest_path)
                rec["status"] = "success"
            except Exception as e:
                logger.exception(f"generating odin manifest failed")
                rec["status"] = "failed"
                tracker.finish_run(run_id, "failed", output_path = OUTPUT_PATH)
                raise e




        with tracker.record_step(run_id, "generating manifest state file") as rec:
            try:
                state_path = odinConfig.manifest_dir / odinConfig.manifest_state_name
                
                
                out_sha = compute_sha256(manifest_path)

                state_doc = ManifestInfo(
                    repo_dir=REPO_DIR,
                    init_qsig=initial_quick_sig,
                    initial_signature_hash= qsig_hex,
                    output_signature_hash=out_sha,
                    timestamp=datetime.now(local_zone).strftime("%Y-%m-%d_%H:%M:%S")
                )
                atomic_write_text(state_path, json.dumps(asdict(state_doc), sort_keys=True, indent=2))
                rec["status"] = "success"

                logger.info(f"odin manifest job completed successfully; output at {OUTPUT_PATH}")
                tracker.finish_run(run_id, "success", 
                           output_path = OUTPUT_PATH,
                           output_sig_hash = state_doc.output_signature_hash)
            except Exception as e:
                rec["failed"]
                tracker.finish_run(run_id, "failed", output_path = OUTPUT_PATH)
                raise e
    return manifest_path


if __name__ == "__main__":
    run()