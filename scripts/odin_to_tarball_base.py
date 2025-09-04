from pathlib import Path
from backuplib.logging import setup_logging, Logger, WithContext
from backuplib.configloader import OdinConfig, load_config
from backuplib.checksumtools import write_sha256_sidecar
from backuplib.filesutil import atomic_write_text
from backuplib.gpgtools import gpg_sign_detached
from backuplib.audit import Tracker
from backuplib.jobstatehelper import get_hash, get_upstream_hash
from pydeclarativelib.pydeclarativelib import make_a_tarball
from pydeclarativelib.declarativeaudit import audited_by
import tarfile
import datetime
from fnmatch import fnmatch
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

def path_matches_any(rel: str, patterns: list[str]) -> bool:
    """Match POSIX-style relative path against glob patterns (supports **)."""
    return any(fnmatch(rel, pat) for pat in patterns)


tracker = Tracker()


@audited_by(tracker, with_step_name="make idempotent copy", and_run_id = run_id)
def make_idempotent_copy(
                    new_tarball_file_path: Path, 
                    tarball_dir_idempotent : Path):
    shutil.copy(new_tarball_file_path, tarball_dir_idempotent)
    return (True, None)




tracker.start_run(run_id=run_id,
                run_name="odin_tarball",
                meta={
                    "timestamp" :  utc_timestamp,
                    "timezone" : odin_cfg.local_zone
                })


def create_tarball(
                repo_dir: Path, 
                exclude_patterns: list[str], 
                dest_tar_gz: Path) -> None:
    """
    Create tar.gz at dest_tar_gz containing repo_dir contents (without nesting under a top folder).
    Excludes are matched against POSIX-style relative paths.
    """
    logger.info(f"Creating tarball: {dest_tar_gz}")
    use_new_method = True
    if use_new_method:
        logger.info("using new make_a_tarball method")
        try:
            make_a_tarball(of_dir= repo_dir, at=dest_tar_gz, excluding=exclude_patterns)
        except:
            logger.exception("could not make tarball using new method")
            raise
    else:    
        with tarfile.open(dest_tar_gz, "w:gz") as tar:
            for p in repo_dir.rglob("*"):
                rel = p.relative_to(repo_dir).as_posix()
                if exclude_patterns and path_matches_any(rel, exclude_patterns):
                    continue
                # Preserve directory structure but avoid including the repo root name.
                tar.add(p, arcname=rel)


def write_state(marker_file : Path,
                latest_tarball : str
                ):
    with marker_file.open("w") as f:
        f.write(latest_tarball + "\n")

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
    logger.info(f"generated idempotent odin backup with datetime {utc_timestamp} and sha245 {hash}")
    atomic_write_text(path=statefile_path, text=f"{json.dumps(state)}\n")
    logger.info(f"generated Odin tarball state file at {statefile_path}")

def main():
    try:
        do_regardless = True

        with tracker.record_step(run_id =run_id, 
                             name = "check for state change"
                             ) as rec:
            try:
                upstream_hash = "A"
                if odin_cfg.tarball_job_upstream_statepath.exists():
                    upstream_hash = get_hash(statefile_path=odin_cfg.tarball_job_upstream_statepath)
                statepath = odin_cfg.tarball_dir_idempotent / odin_cfg.tarball_state_filename
                last_recorded_upstream_hash = "B"
                if statepath.exists():
                    last_recorded_upstream_hash = get_upstream_hash(statefile_path=statepath)
                logger.info(f"upstream_hash: {upstream_hash}; last_recorded_upstream_hash: {last_recorded_upstream_hash}")
                if not do_regardless:
                    if upstream_hash == last_recorded_upstream_hash:
                        logger.info("skipping because no upstream changes")
                        rec["status"] = "success"
                        rec["message"] = "no upstream change: skipping"
                        tracker.finish_run(run_id, "skipped")
                        return
            except:
                logger.exception("error: could not get the upstream hashes")
                rec["status"] = "failed"
                raise

        with tracker.record_step(run_id =run_id, 
                             name = "initial setup"
                             ) as rec:
            try:
                
                repo_dir = odin_cfg.repo_dir
                exclude_list = odin_cfg.tarball_exclusions
                
                tarball_location = odin_cfg.tarball_dir
                tarball_filename = f"Odin_{datestamp}.tar.gz"
                tarball_idempotent_filename = odin_cfg.default_tarball_name
                tarball_path = tarball_location / tarball_filename
                marker_file = tarball_location / "latest.txt"
                tarball_idempotent_path = odin_cfg.tarball_dir_idempotent / tarball_idempotent_filename
                rec["status"] = "success"
            except:
                rec["status"] = "failed"
                raise

        with tracker.record_step(run_id =run_id, 
                             name = "generate tarball"
                             ) as rec:
            try:
                create_tarball(
                    repo_dir=repo_dir,
                    exclude_patterns=exclude_list,
                    dest_tar_gz=tarball_path
                )
                rec["status"] = "success"
            except:
                rec["status"] = "failed"
                raise

        with tracker.record_step(run_id =run_id, 
                             name = "write state file"
                             ) as rec:
            try:
                write_state(marker_file=marker_file, 
                        latest_tarball = str(tarball_path),
                        
                        )
                rec["status"] = "success"
            except:
                rec["status"] = "failed"
                raise
                
        
        make_idempotent_copy(tarball_path, tarball_idempotent_path)

        with tracker.record_step(run_id =run_id, 
                             name = "write idempotent state"
                             ) as rec:
            try:
                
                write_idempotent_state(
                                            tarball_idempotent_path,
                                            upstream_hash=upstream_hash
                                    )
                rec["status"] = "success"
            except:
                rec["status"] = "failed"
                raise

        with tracker.record_step(run_id =run_id, 
                                    name = "sign idempotent copy"
                                    ) as rec:
            try:
                gpg_sign_detached(
                    artifact=tarball_idempotent_path,
                    signer=odin_cfg.recipient,
                    armor = False
                )
                rec["status"] = "success"
            except:
                rec["status"] = "failed"
                raise

        logger.info("SUCCESS")
        tracker.finish_run(run_id, "success", output_path = str(odin_cfg.tarball_dir_idempotent))
    except Exception as e:
        tracker.finish_run(run_id, "failed")
        logger.info("FAILED")
        logger.exception("did not generate the Odin backup tarball successfully")

if __name__ == "__main__":
    main()
