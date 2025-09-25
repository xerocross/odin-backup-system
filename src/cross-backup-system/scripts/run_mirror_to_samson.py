#!/usr/bin/env python3
import subprocess
from typing import Tuple
from pathlib import Path
from backuplib.logging import setup_logging, WithContext
from backuplib.configloader import OdinConfig, load_config
from backuplib.audit import Tracker
from backuplib.filesutil import quick_scan_signature, QuickManifestScan, \
      hash_quick_manifest_scan, file_to_lines_list
from backuplib.checksumtools import sha256_string
from typing import List
import datetime
import uuid
import traceback
from dataclasses import dataclass

@dataclass
class MirrorListItem:
    origin: Path
    target: Path


def run(parent_id : str | None = None):
    try:
        odin_cfg: OdinConfig = load_config()
        logger = setup_logging(appName="rsync_mirroring")
        run_id = "odin-rsync-mirror-"+str(uuid.uuid4())
        logger = WithContext(logger, {"run_id": run_id})
        if parent_id is not None:
            logger = WithContext(logger, {"parent_id": parent_id})
        utc_timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat()
        tracker = Tracker()
        trace : List[str] = []

        def generate_signature(souce_list : List[str], exclude_patterns: List[str]):
            hashes = []
            souce_list.sort()
            for item in souce_list:
                qsig = quick_scan_signature(root=item, exclude = exclude_patterns)
                hashes.append(hash_quick_manifest_scan(quick_scan=qsig))
            hashes_string = "".join(hashes)
            signature = sha256_string(hashes_string)
            return signature


        def read_rsync_mirroring_file(rsync_mirroring_file: Path):
            mirroring_list = []
            with open(rsync_mirroring_file, 'r') as f:
                for line in f.readlines():
                    dir = line.strip()
                    mirroring_list.append(dir)
            return mirroring_list

        def get_source_destination_list() -> List[MirrorListItem]:
            rsync_mirroring_file = odin_cfg.repo_dir / odin_cfg.rsync_mirroring_file
            config_targets : List[str] = read_rsync_mirroring_file(rsync_mirroring_file)
            source_destination_list :List[MirrorListItem] = []
            source_root = Path(odin_cfg.sidecar_root)
            dest_root = Path(odin_cfg.repo_dir)
            for t in config_targets:
                mirror_item : MirrorListItem = MirrorListItem(origin=source_root / t
                                                            , target=dest_root / t)
                source_destination_list.append(mirror_item)
            return source_destination_list

        def apply_rsync_to_list(source_destination_list: List[MirrorListItem]):
            exclude_file = odin_cfg.repo_dir / odin_cfg.rsync_exclusions_file
            for item in source_destination_list:
                rsync(item.origin, item.target, exclude_file = exclude_file)

        def rsync( source, target, exclude_file = None):
            logger.info(f"attempting to rsync from {str(source)} to {str(target)}")
            cmd = ["rsync","-aHAX","--delete","--human-readable", "--out-format=%t %i %n%L", "--itemize-changes" ,"--info=STATS2"]

            if exclude_file is not None:
                cmd += ["--exclude-from", str(Path(exclude_file).expanduser().resolve())]
            cmd += [str(source), str(target)]
            try:
                with subprocess.Popen(
                        cmd,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.STDOUT,  # capture stderr too (rsync prints a lot here)
                        text=True,
                        bufsize=1,
                    ) as p:
                        log_msg = ""
                        if p.stdout is not None:
                            for line in p.stdout:
                                log_msg = log_msg + line.rstrip() + "; "
                        if p.stderr is not None:
                            for line in p.stderr:
                                log_msg = log_msg + line.rstrip() + "; "
                        logger.info("rsync log:" + log_msg)
                        rc = p.wait()
                        if rc != 0:
                            raise subprocess.CalledProcessError(rc, cmd)
                logger.info(f"executed {cmd}")
            except:
                logger.exception(f"could not execute rsync command: {cmd}")

        tracker.start_run(
                    run_id=run_id,
                    run_name="odin_rsync_mirror_job",
                    meta={
                        "timestamp" :  utc_timestamp,
                        "timezone" : odin_cfg.local_zone
                    }
                )
        
        if parent_id is not None:
            tracker.set_parent_id(run_id=run_id, parent_id=parent_id)

        logger.info("starting rsync mirror job")
        mirrorlist : List[MirrorListItem] = get_source_destination_list()
        souce_list = [i.origin for i in mirrorlist]

        exclude_file = odin_cfg.repo_dir / odin_cfg.rsync_exclusions_file
        exclusions = file_to_lines_list(from_file=exclude_file)
        current_signature =  generate_signature(souce_list=souce_list, exclude_patterns=exclusions)
        logger.info(f"current signature: {current_signature}")
        
        apply_rsync_to_list(mirrorlist)
        logger.info("completed rsync jobs")
        tracker.finish_run(run_id, "success")
        return {"success": True}
    except Exception as e:
        logger.exception("job failed")
        tracker.finish_run(run_id, "failed")
        raise e

if __name__ == "__main__":
    run(parent_id=None)
