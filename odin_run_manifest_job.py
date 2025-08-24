#!/usr/bin/env python3
from __future__ import annotations
from pathlib import Path
import os, tempfile
from backuplib.generate_manifest import write_manifest
from backuplib.audit import Tracker
from backuplib.filesutil import quick_scan_signature, digest, atomic_write_text, QuickManifestSig
from backuplib.checksumtools import compute_sha256
import json
from zoneinfo import ZoneInfo
from datetime import datetime
from dataclasses import dataclass
from typing import Dict, List, str
from backuplib.logging import setup_logging, WithContext
from dataclasses import asdict
import uuid
import yaml

global_log = setup_logging(level="INFO", appName="odin_generate_manifest") 

class ConfigError(Exception):
    '''Encountered an error in the configuration'''
    pass

# Load a YAML file into a Python dict
CONFIG_PATH = "~/.config/odin/touch odin_run_manifest_job.yaml"
with open(CONFIG_PATH, "r") as f:
    config = yaml.safe_load(f)

try:
    print(config["database"]["host"])
    print(config["database"]["port"])
    REPO_DIR = config["REPO_DIR"]
    ODIN_MANIFEST_DIR = config["ODIN_MANIFEST_DIR"]
    EXCLUSIONS = config["EXCLUSIONS"]
except KeyError as e:
    raise ConfigError(f"Missing required config key. Config path: {CONFIG_PATH}.")

local_zone = ZoneInfo("America/New_York")

LOGFILE = "/home/adam/odin-plaintext-backups.log"

@dataclass
class ManifestInfo:
    root_path: str
    init_qsig : QuickManifestSig
    age: int
    city: str

def localtimestamp() -> str:
    return datetime.now(local_zone).strftime("%Y-%m-%d_%H%M%S")

def odin_run_manifest_job():
    run_id = "manifest-" + str(uuid.uuid4())
    log_run_id = str(run_id)[:12]
    log = WithContext(global_log, {"run_log_id": log_run_id})
    log.info(f"starting the odin manifest job run_id: {run_id}")

    root = Path(REPO_DIR)
    out_dir = Path(ODIN_MANIFEST_DIR)
    tracker = Tracker()
    timestamp = localtimestamp()
    run_id = run_id

    
    out_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = out_dir / f"odin.manifest.yaml"
    state_path = manifest_path.with_suffix(manifest_path.suffix + ".state.json")
    
    qsig : QuickManifestSig = quick_scan_signature(root=root, exclude=EXCLUSIONS)
    qsig_json = json.dumps(asdict(qsig))
    qsig_hex = digest(qsig)
    

    tracker.start_run(run_id,
                      run_name="generate_manifest",
                      input_sig_json = qsig_json,
                      meta={"job": "generate manifest", 
                                    "root": str(root),
                                    "timestamp" :  timestamp,
                            })

    with tracker.record_step(
                            run_id=run_id, 
                            name="check signature", 
                            input_sig=qsig, output_path=manifest_path
                    ) as rec:
        if manifest_path.exists() and state_path.exists():
            try:
                state = json.loads(state_path.read_text(encoding="utf-8"))
                if state.get("input_sig_hex") == qsig_hex:
                    # Optional extra safety: verify the manifest hash still matches

                    if state.get("output_sig_hex") == compute_sha256(manifest_path):
                        # record 'skipped' in the audit DB and return
                        
                        rec["status"] = "skipped"
                        rec["output_sig"] = {"sha256": state["output_sig_hex"]}
                        tracker.finish_run(run_id, "success")
                        return manifest_path
            except Exception:
                # TODO implement this
                # fall through to rebuild if state is unreadable
                pass
                

    with tracker.record_step(run_id, "compute manifest", input_sig=qsig, output_path=manifest_path) as rec:
        write_manifest_helper(
            manifest_path,
            tracker = tracker,
            run_id = run_id
        )

def write_manifest_helper(manifest_path,
                          *,
                          tracker,
                          run_id: str,
                          initial_quick_sig: QuickManifestSig,
                          qsig_hex: str):

    with tempfile.TemporaryDirectory() as tmpdir:
        print("Working in:", tmpdir)
        manifest_temp_file_path = os.path.join(tmpdir, "manifest.yaml")
        write_manifest(
                        root_dir=REPO_DIR, 
                        manifest_path = manifest_temp_file_path, 
                        format_type="yaml", 
                        exclude_patterns=EXCLUSIONS
                    )
            
        os.replace(manifest_temp_file_path, manifest_path)
        #audit_rec["status"] = "computed manifest file"
        #audit_rec["output manifest path"] = manifest_path

        state_path = manifest_path.with_suffix(manifest_path.suffix + ".state.json")

        out_sha = compute_sha256(manifest_path)

        state_doc = ManifestInfo(
            input_sig=initial_quick_sig,
            input_sig_hex= qsig_hex,
            output_sig_hex=out_sha,
            generated_timestamp=datetime.now(local_zone).strftime("%Y-%m-%d_%H:%:M:%S")
        )
        atomic_write_text(state_path, json.dumps(state_doc, sort_keys=True, indent=2))
        #audit_rec["output_sig"] = {"sha256": out_sha}
        tracker.finish_run(run_id, "success")
        manifest_path


if __name__ == "__main__":
    odin_run_manifest_job()