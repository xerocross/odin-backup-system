#!/usr/bin/env python3
from __future__ import annotations
from pathlib import Path
import os, tempfile
from backuplib.generate_manifest import write_manifest
from backuplib.audit import Tracker
from backuplib.filesutil import quick_scan_signature, atomic_write_text, QuickManifestSig
from backuplib.checksumtools import compute_sha256, digest
from backuplib.exceptions import ConfigException
import json
from zoneinfo import ZoneInfo
from datetime import datetime
from dataclasses import dataclass
from typing import Dict, List
from backuplib.logging import setup_logging, WithContext
from backuplib.configloader import OdinConfig, load_config
from dataclasses import asdict
import uuid
import yaml

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
    root_path: str
    init_qsig : QuickManifestSig
    init_sig_hex: str
    output_sig_hex: str
    timestamp: str

def localtimestamp() -> str:
    return datetime.now(local_zone).strftime("%Y-%m-%d_%H%M%S")

def get_manifest_state():


    with open(config_dict["STATE_PATH"], 'r', encoding="utf-8") as f:
        data = json.load(f)
        return data



def run():
    odin_run_manifest_job()

def odin_run_manifest_job():
    
    try:
        run_id = "manifest-" + str(uuid.uuid4())
        log_run_id = str(run_id)[:12]
        log = WithContext(global_log, {"run_log_id": log_run_id})
        log.info(f"starting the odin manifest job run_id: {run_id}")

        root = Path(REPO_DIR)
        out_dir = Path(ODIN_MANIFEST_DIR)
        tracker = Tracker()
        log.info("made tracker")
        out_dir.mkdir(parents=True, exist_ok=True)
        manifest_path = out_dir / f"odin.manifest.yaml"
        state_path = manifest_path.with_suffix(manifest_path.suffix + ".state.json")
        log.info(f"found state path: {state_path}")
        qsig : QuickManifestSig = quick_scan_signature(root=root, exclude=EXCLUSIONS)
        qsig_json = json.dumps(asdict(qsig))
        qsig_hex = digest(asdict(qsig))
        log.info(f"computed qsig hex: {qsig_hex}")
        tracker.start_run(run_id = run_id,
                            run_name="generate_manifest",
                            input_sig_json = qsig_json,
                            meta={
                                        "job": "generate manifest", 
                                        "root": str(root)
                            })
    except Exception as e:
        log.exception("something bad happened")
    

    

    with tracker.record_step(
                            run_id=run_id, 
                            name="check signature"
                            ) as rec:
        if manifest_path.exists() and state_path.exists():
            log.info("found that manifest exists")
            try:
                


                state = json.loads(state_path.read_text(encoding="utf-8"))
                state_init_sig_hex = state.get("init_sig_hex")
                log.info(f"found that state init siq hex is {state_init_sig_hex}")
                if state_init_sig_hex == qsig_hex:
                    # Optional extra safety: verify the manifest hash still matches
                    previous_state_sig = state.get("output_sig_hex")
                    current_state_sig = compute_sha256(manifest_path)
                    log.info(f"found previous state sig {previous_state_sig} and current state sig {current_state_sig}")
                    if previous_state_sig == compute_sha256(manifest_path):
                        # record 'skipped' in the audit DB and return
                        log.info("found existing state matches current: skipping")
                        rec["status"] = "skipped"
                        tracker.finish_run(run_id, "skipped", output_path = ODIN_MANIFEST_DIR)
                        return manifest_path
                    else:
                        log.info("current state sig is new")
                        write_manifest_helper(
                            manifest_path,
                            tracker = tracker,
                            run_id = run_id,
                            log = log,
                            initial_quick_sig=qsig,
                            qsig_hex=qsig_hex,
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
                state_path = manifest_path.with_suffix(manifest_path.suffix + ".state.json")
                out_sha = compute_sha256(manifest_path)

                state_doc = ManifestInfo(
                    root_path=REPO_DIR,
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
    return manifest_path


if __name__ == "__main__":
    odin_run_manifest_job()