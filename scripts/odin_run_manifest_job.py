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
                                manifestInfo: ManifestInfo
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
            log.info(f"odin manifest job completed successfully; output at {OUTPUT_PATH}")
            tracker.finish_run(run_id, "success", 
                        output_path = OUTPUT_PATH,
                        output_sig_hash = state_doc.output_sig_hex)
        except Exception as e:
            rec["failed"]
            tracker.finish_run(run_id, "failed", output_path = OUTPUT_PATH)
            raise e
    pass


def run():
    odin_run_manifest_job()



def perform_initial_job_setup(odinConfig: OdinConfig) -> tuple[ManifestInfo, logging.Logger, Tracker]:
    run_id = "manifest-" + str(uuid.uuid4())
    log = WithContext(global_log, {"log_id": run_id})
    
    log.info(f"starting the odin manifest job run_id: {run_id}")
    out_dir = Path(ODIN_MANIFEST_DIR)
    tracker = Tracker()

    out_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = odinConfig.manifest_dir / odinConfig.manifest_file_name
    
    state_path = odinConfig.manifest_dir / odinConfig.manifest_state_name

    log.debug(f"found state path: {state_path}")
    quick_signature : QuickManifestSig = quick_scan_signature(
                                            root = odinConfig.repo_dir, 
                                            exclude=odinConfig.manifest_exclusions
                                        )
    initial_signature_hash = digest(asdict(quick_signature))
    log.info(f"computed initial quick-signature hash: {initial_signature_hash}")

    manifestInfo = ManifestInfo(
        run_id = run_id,
        repo_dir = odinConfig.repo_dir,
        init_qsig = quick_signature,
        initial_signature_hash = initial_signature_hash,
        manifest_path = manifest_path,
        state_path = state_path
    )
    return manifestInfo, log, tracker


def get_manifest_path(odinConfig : OdinConfig) -> Path:
    return odinConfig.manifest_dir / odinConfig.manifest_file_name

def get_state_path(odinConfig : OdinConfig) -> Path:
    return odinConfig.manifest_dir / odinConfig.manifest_state_name

def decide_whether_to_redo_this_job(
                                    odinConfig : odinConfig,
                                    tracker : Tracker,
                                    manifestInfo: ManifestInfo,
                                    logger : Logger
        ):
    run_id = manifestInfo.run_id
    with tracker.record_step(
                            run_id=run_id, 
                            name="check signature"
                            ) as rec:
        
        manifest_path = get_manifest_path(odinConfig)
        manifestInfo.manifest_path = manifest_path
        state_path = get_state_path(odinConfig)
        manifestInfo.state_path = state_path
        
        if manifest_path.exists() and state_path.exists():
            logger.debug("found that manifest and manifest state exist")
            try:
                
                state = json.loads(state_path.read_text(encoding="utf-8"))
                
                state_init_sig_hex = state.get("init_sig_hex")
                
                logger.info(f"found that state init siq hex is {state_init_sig_hex}")
                
                if state_init_sig_hex == manifestInfo.initial_signature_hash:
                    # Optional extra safety: verify the manifest hash still matches
                    previous_state_sig = state.get("output_sig_hex")
                    current_state_sig = compute_sha256(manifest_path)
                    logger.info(f"found previous state sig {previous_state_sig} and current state sig {current_state_sig}")
                    if previous_state_sig == compute_sha256(manifest_path):
                        # record 'skipped' in the audit DB and return
                        logger.info("found existing state matches current: skipping")
                        rec["status"] = "skipped"
                        tracker.finish_run(run_id, "skipped", output_path = ODIN_MANIFEST_DIR)
                        return manifest_path
                    else:
                        logger.info("current state sig is new")
                        write_manifest_helper(
                            manifest_path,
                            tracker = tracker,
                            log = logger,
                            manifestInfo= manifestInfo,
                        )
                else:
                    log.info("state info not found:")
                    write_manifest_helper(
                        manifest_path,
                        tracker = tracker,
                        run_id = run_id,
                        log = log,
                        initial_quick_sig=qsig,
                        qsig_hex=qsig_hex,
                    )
            except Exception as e:
                rec["status"] = "failed"
                msg = "encountered an exception while checking state: falling back on re-build"
                rec["message"] = msg
                log.exception(msg)
                log.info("beginning write_manifest_helper")
                write_manifest_helper(
                    manifest_path,
                    tracker = tracker,
                    run_id = run_id,
                    log = log,
                    initial_quick_sig=qsig,
                    qsig_hex=qsig_hex,
                )
        else:
            log.info("found no existing manifest or state")
            write_manifest_helper(
                manifest_path,
                tracker = tracker,
                run_id = run_id,
                initial_quick_sig=qsig,
                qsig_hex=qsig_hex,
                log = log
            )



def odin_run_manifest_job():
    manifestInfo, log, tracker = perform_initial_job_setup(odinConfig)
    run_id = manifestInfo.run_id
    tracker.start_run(run_id = run_id,
                        run_name="generate_manifest",
                        input_sig_json = manifestInfo.init_qsig,
                        meta={
                            "job": "generate manifest", 
                            "repo_dir": str(manifestInfo.repo_dir)
                        })


    




def write_manifest_helper(manifest_path,
                          *,
                          tracker,
                          run_id: str,
                          initial_quick_sig: QuickManifestSig,
                          qsig_hex: str,
                          log):

    with tempfile.TemporaryDirectory() as tmpdir:
        print("Working in:", tmpdir)

        with tracker.record_step(run_id, "generating odin manifest") as rec:
            manifest_temp_file_path = os.path.join(tmpdir, "manifest.yaml")
            log.info("starting to generate manifest")
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
                log.exception("encountered an exception while generating/writing the manifest")
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

                log.info(f"odin manifest job completed successfully; output at {OUTPUT_PATH}")
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