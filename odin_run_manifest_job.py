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

REPO_DIR = "/home/adam/OdinBack2"
ODIN_MANIFEST_DIR = "/home/adam/odin_manifest"
local_zone = ZoneInfo("America/New_York")

LOGFILE = "/home/adam/odin-plaintext-backups.log"
EXCLUSIONS = ["*venv", 
              "*venv/**", 
              "*node_modules*", 
              "*__pycache__*",
              "*.pyc",
              "*.git*"
            ]

@dataclass
class ManifestInfo:
    root_path: str
    init_qsig : QuickManifestSig
    age: int
    city: str

def localtimestamp() -> str:
    return datetime.now(local_zone).strftime("%Y-%m-%d_%H%M%S")


def odin_run_manifest_job():
    root = Path(REPO_DIR)
    out_dir = Path(ODIN_MANIFEST_DIR)
    tracker = Tracker()
    timestamp = localtimestamp()
    run_id = timestamp
    tracker.start_run(run_id, meta={"job": "generate manifest", 
                                    "root": str(root),
                                    "timestamp" :  timestamp,
                                    "name": out_dir.name})


    out_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = out_dir / f"odin.manifest.yaml"
    state_path = manifest_path.with_suffix(manifest_path.suffix + ".state.json")
    
    qsig : QuickManifestSig = quick_scan_signature(root=root, exclude=EXCLUSIONS)
    qsig_hex = digest(qsig)
    
    with tracker.record_step(run_id, "check signature", 
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
        with tempfile.TemporaryDirectory() as tmpdir:
            print("Working in:", tmpdir)
            manifest_temp_file_path = os.path.join(tmpdir, "manifest.yaml")
            write_manifest(root_dir=REPO_DIR, 
                        manifest_path = manifest_temp_file_path, 
                       format_type="yaml", 
                       exclude_patterns=EXCLUSIONS)
            
            os.replace(manifest_temp_file_path, manifest_path)
        rec["status"] = "computed manifest file"
        rec["output manifest path"] = manifest_path
        state_path = manifest_path.with_suffix(manifest_path.suffix + ".state.json")

        out_sha = compute_sha256(manifest_path)
        state_doc = {
            "input_sig": qsig,
            "input_sig_hex": qsig_hex,
            "output_sig_hex": out_sha,
            "generated_timestamp": datetime.now(local_zone).strftime("%Y-%m-%d_%H:%:M:%S")
        }
        atomic_write_text(state_path, json.dumps(state_doc, sort_keys=True, indent=2))
        rec["output_sig"] = {"sha256": out_sha}
        tracker.finish_run(run_id, "success")
    return manifest_path


def write_manifest_helper(manifest_path,
                          *,
                          tracker,
                          run_id: str,
                          audit_rec, 
                          initial_quick_sig: QuickManifestSig,
                          qsig_hex: str):

    with tempfile.TemporaryDirectory() as tmpdir:
        print("Working in:", tmpdir)
        manifest_temp_file_path = os.path.join(tmpdir, "manifest.yaml")
        write_manifest(root_dir=REPO_DIR, 
                        manifest_path = manifest_temp_file_path, 
                       format_type="yaml", 
                       exclude_patterns=EXCLUSIONS)
            
        os.replace(manifest_temp_file_path, manifest_path)
        audit_rec["status"] = "computed manifest file"
        audit_rec["output manifest path"] = manifest_path
        state_path = manifest_path.with_suffix(manifest_path.suffix + ".state.json")

        out_sha = compute_sha256(manifest_path)

        state_doc = ManifestInfo(
            input_sig=initial_quick_sig,
            input_sig_hex= qsig_hex,
            output_sig_hex=out_sha,
            generated_timestamp=datetime.now(local_zone).strftime("%Y-%m-%d_%H:%:M:%S")
        )
        atomic_write_text(state_path, json.dumps(state_doc, sort_keys=True, indent=2))
        audit_rec["output_sig"] = {"sha256": out_sha}
        tracker.finish_run(run_id, "success")
    pass


if __name__ == "__main__":
    odin_run_manifest_job()