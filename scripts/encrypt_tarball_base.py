from pathlib import Path
from backuplib.configloader import OdinConfig, load_config
from typing import Iterable, Optional
import subprocess
import datetime
from zoneinfo import ZoneInfo
import os, tempfile
from backuplib.logging import setup_logging, Logger, WithContext
from backuplib.checksumtools import sha256_file
from backuplib.jobstatehelper import get_upstream_hash, get_hash
from backuplib.filesutil import atomic_write_text
from backuplib.audit import Tracker, RunSignature
import json
import shutil
import uuid


odinConfig: OdinConfig = load_config()
logger : Logger = setup_logging()
run_id = "odin-encrypt-"+str(uuid.uuid4())
logger = WithContext(logger, {"run_id": run_id})
utc_timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat()

tracker = Tracker()

tracker.start_run(run_id=run_id,
                run_name="odin_encrypt_tarball",
                meta={
                    "timestamp" :  utc_timestamp,
                    "timezone" : odinConfig.local_zone
                })

class GPGError(RuntimeError):
    pass


def write_state_file(statefile_path: Path, new_run_signature: str, upstream_hash : str):
    try:
        logger.info("writing state file for odin encrypted tarball job")
        state = {
            "hash" : new_run_signature,
            "datetime": utc_timestamp,
            "upstream_hash": upstream_hash
        }
        atomic_write_text(path = statefile_path, text = f"{json.dumps(state)}\n")
        logger.info(f"state file for odin encrypted tarball written to {statefile_path}")
    except:
        logger.exception("could not generated state file for encrypted tarball job")

def encrypt_tarball_to_recipient(
    tarball: Path | str,
    recipient: str,
    out_path: Path | str,
    *,
    gpg_binary: str = "gpg",
    armor: bool = False,
    extra_args: Optional[Iterable[str]] = None,
    overwrite: bool = True,
) -> Path:
    """
    Encrypt `tarball` to the given public-key `recipient` (must exist in your GPG keyring).
    Returns the output path.
    """
    tarball = Path(tarball)
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    if not tarball.is_file():
        raise FileNotFoundError(f"Tarball not found: {tarball}")

    if out_path.exists() and not overwrite:
        raise FileExistsError(f"Refusing to overwrite: {out_path}")

    cmd = [gpg_binary, "--batch", "--yes", "--encrypt", "--recipient", recipient, "-o", str(out_path)]
    if armor:
        cmd.append("--armor")
    if extra_args:
        cmd.extend(extra_args)

    # Input file comes last
    cmd.append(str(tarball))

    try:
        res = subprocess.run(cmd, capture_output=True, text=True, check=False)
    except FileNotFoundError as e:
        raise GPGError(f"Could not find gpg binary '{gpg_binary}'. Is GnuPG installed?") from e

    if res.returncode != 0:
        raise GPGError(f"GPG failed (code {res.returncode}).\nSTDERR:\n{res.stderr.strip()}")

    return out_path


def read_latest_tarball(marker_file: str) -> Path:
    marker_path = Path(marker_file)  # normalize the marker file path itself
    with marker_path.open("r") as f:
        line = f.readline().strip()
    return Path(line)  # system-independent Path object

def copy_tarball_to_dropbox(encrypted_tarball_path : Path, timestamp : str):
    new_folder_for_sync = odinConfig.offsite_sync_dir / f"OdinVault-{timestamp}"
    encrypted_file_name = odinConfig.encrypted_tarball_name
    new_folder_for_sync.mkdir(parents=True, exist_ok=True)
    shutil.copy(encrypted_tarball_path, new_folder_for_sync / encrypted_file_name)
    logger.info(f"copied encrypted tarball to {new_folder_for_sync}")

def main():
    try:
        statefile_path = odinConfig.encryption_job.dir / odinConfig.encryption_job.statefile_name
        previous_job_signature = get_hash(odinConfig.encryption_job.upstream_statepath)
        tracker.set_signature_data(run_id = run_id, 
                                           signature_data=previous_job_signature, 
                                           column=RunSignature.PREVIOUS_JOB_SIGNATURE)
        
        tz = ZoneInfo(odinConfig.local_zone)
        timestamp = datetime.datetime.now(tz).strftime("%Y_%m_%d-%H%M")
        
        with tracker.record_step(run_id =run_id, 
                                    name = "compare state hashes"
                                    ) as rec:
        
            try:
                upstream_hash = get_hash(odinConfig.encryption_job.upstream_statepath)
                previous_run_upstream_hash = get_upstream_hash(statefile_path)
                tracker.set_signature_data(run_id = run_id, 
                                           signature_data=upstream_hash, 
                                           column=RunSignature.CURRENT_UPSTREAM_SIGNATURE)
                tracker.set_signature_data(run_id = run_id, 
                                           signature_data=previous_run_upstream_hash, 
                                           column=RunSignature.PREVIOUS_UPSTREAM_SIGNATURE)
                if upstream_hash == previous_run_upstream_hash:
                    logger.info("there was no upstream change recorded: skipping")
                    tracker.finish_run(run_id, "skipped")
                    rec["message"] = "no upstream change"
                    return
                rec["status"] = "success"
            except:
                rec["status"] = "failed"
                raise


        #tracker.set_input_sig_hash(run_id=run_id, input_sig_hash : str):

        tarball_dir = odinConfig.tarball_dir_idempotent
        latest_tarball_path = tarball_dir / odinConfig.default_tarball_name
        logger.info(f"read that latest odin tarball is at {latest_tarball_path}")
        if not latest_tarball_path.exists():
            logger.error(f"could not find what was supposed to be the the most recent odin tarball: {latest_tarball_path}")
            raise FileNotFoundError(f"File does not exist: {latest_tarball_path}")

        encrypted_odin_path = odinConfig.encrypted_dir
        encrypted_file_name = odinConfig.encrypted_tarball_name
        encrypted_tarball_path = encrypted_odin_path / encrypted_file_name

        with tracker.record_step(run_id =run_id, 
                                    name = "generate encrypted_tarball"
                                    ) as rec:
            try:
                with tempfile.TemporaryDirectory() as tmpdir:
                    out_path = Path(tmpdir) / encrypted_file_name

                    encrypted_tarball = encrypt_tarball_to_recipient (
                        tarball=latest_tarball_path,
                        recipient=odinConfig.recipient,
                        out_path=out_path
                    )
                    os.replace(encrypted_tarball, encrypted_tarball_path)
                    logger.info(f"wrote new encrypted tarball of odin to {encrypted_tarball_path}")
                    rec["status"] = "success"
            except:
                rec["status"] = "failed"
                raise
        
        with tracker.record_step(run_id =run_id, 
                                    name = "copy to dropbox"
                                    ) as rec:
            try:
                copy_tarball_to_dropbox(encrypted_tarball_path, timestamp=timestamp)
                rec["status"] = "success"
            except:
                rec["status"] = "failed"
                raise

        with tracker.record_step(run_id =run_id, 
                                            name = "write statefile"
                                            ) as rec:
            try:
                new_run_signature = sha256_file(encrypted_tarball_path)
                write_state_file(statefile_path = statefile_path, 
                                 new_run_signature = new_run_signature,
                                 upstream_hash=upstream_hash)
                rec["status"] = "success"
            except:
                rec["status"] = "failed"
                raise

        tracker.set_signature_data(run_id = run_id, 
                                           signature_data=new_run_signature, 
                                           column=RunSignature.JOB_RESULT_SIGNATURE)
        tracker.finish_run(run_id, "success")
        logger.exception("success")
    except:
        tracker.finish_run(run_id, "failed")
        logger.exception("was not able to create the encrypted odin backup")


if __name__ == "__main__":
    main()