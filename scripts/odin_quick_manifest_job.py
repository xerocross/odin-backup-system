
from backuplib.configloader import OdinConfig, load_config
from backuplib.filesutil import QuickManifestScan, quick_scan_signature,\
      hash_quick_manifest_scan, atomic_write_text
from backuplib.jobstatehelper import get_hash
from backuplib.logging import setup_logging, Logger, WithContext
from backuplib.checksumtools import sha256_string
from backuplib.audit import Tracker
from pydeclarativelib.declarativeaudit import audited_by
from pydeclarativelib.declarativesuccess import with_try_except_and_trace
from backuplib.backupjob import BackupJobResult
from typing import Tuple
from dataclasses import asdict
from pathlib import Path
import datetime
import json
import uuid

tracker = Tracker()
trace = []

odin_config : OdinConfig = load_config()
utc_timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat()
run_id = "odin-quick-manifest-"+str(uuid.uuid4())
logger = setup_logging(appName = "quick_manifest_job")
logger = WithContext(logger, {"run_id": run_id})


tracker.start_run(run_id=run_id,
                run_name="odin_tarball",
                meta={
                    "timestamp" :  utc_timestamp,
                    "timezone" : odin_config.local_zone
                })

@audited_by(tracker, with_step_name="write state", and_run_id = run_id)
@with_try_except_and_trace(if_success_then_message="wrote state", if_failed_then_message="could not write state", with_trace=trace)
def write_state(
                statefile_path : Path,
                quick_manifest_scan_hash : str,
                upstream_hash : str
            ):
    statefile_name = odin_config.quick_manifest_config.statefile_name
    statefile_dir = odin_config.quick_manifest_config.dir
    statefile_path = statefile_dir / statefile_name
    state = {
        "datetime" : utc_timestamp,
        "hash" : quick_manifest_scan_hash,
        "upstream_hash": upstream_hash
    }
    atomic_write_text(
                        path=statefile_path,
                        text=f"{json.dumps(state)}\n"
                    )
    logger.info(f"generated Odin quick manifest state file at {statefile_path}")
    return {
        "success": True,
        "message": ""
    }

@audited_by(tracker, with_step_name="write quickmanifest", and_run_id = run_id)
@with_try_except_and_trace(if_success_then_message="wrote quickmanifest", 
                           if_failed_then_message="could not write quickmanifest", with_trace=trace)
def write_quickmanifest(quickmanifestscan: QuickManifestScan):
    
    qsigdict = {
            "root": str(quickmanifestscan.root),
            "exclude": quickmanifestscan.exclude,
            "file_count": quickmanifestscan.file_count,
            "latest_mtime_ns" : quickmanifestscan.latest_mtime_ns,
            "total_bytes": quickmanifestscan.total_bytes
    }
    outpath = odin_config.quick_manifest_config.dir / odin_config.quick_manifest_config.outfile
    atomic_write_text(path=outpath, text=f"{json.dumps(qsigdict)}\n")
    logger.info(f"generated Odin quick manifest data file at {outpath}")
    return {
        "success": True,
        "message": ""
    }

def run():
    try:
        upstream_job_hash = ""
        repo_dir = odin_config.repo_dir
        statefile_name = odin_config.quick_manifest_config.statefile_name
        statefile_dir = odin_config.quick_manifest_config.dir
        statefile_path = statefile_dir / statefile_name
        if statefile_path.exists():
            previous_run_upstream_hash = get_hash(statefile_path=statefile_path)
            upstream_job_hash = get_hash(odin_config.quick_manifest_config.upstream_statepath)
            logger.info(f"previous_run_upstream_hash:{previous_run_upstream_hash}; upstream_job_hash: {upstream_job_hash}")
            if previous_run_upstream_hash == upstream_job_hash:
                logger.info("no upstream changes: skipping job")
                logger.info(f"{BackupJobResult.SKIPPED}")
                tracker.finish_run(run_id, "skipped")
                return
        qscan: QuickManifestScan = quick_scan_signature(root = repo_dir, exclude = odin_config.manifest_exclusions)
        hash = sha256_string(f"{hash_quick_manifest_scan(quick_scan = qscan)}:{upstream_job_hash}") 
        write_state(statefile_path=statefile_path, quick_manifest_scan_hash = hash, upstream_hash=upstream_job_hash)
        write_quickmanifest(qscan)
        logger.info(f"{BackupJobResult.SUCCESS}")
        tracker.finish_run(run_id, "success")
    except:
        logger.exception(f"{BackupJobResult.FAILED}")
        tracker.finish_run(run_id, "failed")

if __name__ == "__main__":
    run()