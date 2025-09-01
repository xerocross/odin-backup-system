#!/usr/bin/env python3
import argparse, subprocess, sys
from pathlib import Path
from backuplib.logging import setup_logging, WithContext
from backuplib.configloader import OdinConfig, load_config
from typing import List
import uuid

odin_cfg: OdinConfig = load_config()
logger = setup_logging(appName="rsync_mirroring")
run_id = "odin-rsync-mirror-"+str(uuid.uuid4())
logger = WithContext(logger, {"run_id": run_id})


def read_rsync_mirroring_file(rsync_mirroring_file: Path):
    mirroring_list = []
    with open(rsync_mirroring_file, 'r') as f:
        for line in f.readlines():
            dir = line.strip()
            mirroring_list.append(dir)
    return mirroring_list

def get_source_destination_list():
    rsync_mirroring_file = odin_cfg.repo_dir / odin_cfg.rsync_mirroring_file
    config_targets : List[str] = read_rsync_mirroring_file(rsync_mirroring_file)
    source_destination_list : List[(Path, Path)] = [] 
    source_root = Path(odin_cfg.sidecar_root)
    dest_root = Path(odin_cfg.repo_dir)
    dest_root.as_posix
    for t in config_targets:
        source_destination_list.append((source_root / t, dest_root / t))
    return source_destination_list

def apply_rsync_to_list(source_destination_list):
    exclude_file = odin_cfg.repo_dir / odin_cfg.rsync_exclusions_file
    for source , target in source_destination_list:
        rsync(source, target, exclude_file = exclude_file)

def rsync( source, target, exclude_file = None):
    logger.info(f"attempting to rsync from {str(source)} to {str(target)}")
    cmd = ["rsync","-aHAX","--delete","--human-readable", "--out-format=%t %i %n%L", "--itemize-changes" ,"--info=STATS2,PROGRESS2"]

    if exclude_file is not None:
        cmd += ["--exclude-from", str(Path(exclude_file).expanduser().resolve())]
    cmd += [str(source), str(target)]
    try:
        #c = subprocess.call(cmd)

        with subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,  # capture stderr too (rsync prints a lot here)
                text=True,
                bufsize=1,
            ) as p:
                log_msg = ""
                for line in p.stdout:
                    log_msg = log_msg + line.rstrip() + "; "
                logger.info("rsync log:" + log_msg)
                rc = p.wait()
                if rc != 0:
                    raise subprocess.CalledProcessError(rc, cmd)
        logger.info(f"executed {cmd}")
    except:
        logger.exception(f"could not execute rsync command: {cmd}")

def run():
    logger.info("starting rsync backup of odin")
    mirrorlist = get_source_destination_list()
    apply_rsync_to_list(mirrorlist)
    logger.info("completed rsync jobs")

if __name__ == "__main__":
    run()
