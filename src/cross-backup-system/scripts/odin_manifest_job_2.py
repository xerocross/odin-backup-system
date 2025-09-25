from dataclasses import dataclass, asdict
from pathlib import Path
from backuplib.generate_manifest import write_manifest
from backuplib.logging import setup_logging, WithContext, Logger
from backuplib.configloader import OdinConfig, load_config
from backuplib.backupjob import JobState
from backuplib.jobstatehelper import get_hash, get_upstream_hash
from backuplib.backupjob import BackupJobResult
from backuplib.audit import Tracker, RunSignature
from backuplib.checksumtools import sha256_file
from pydeclarativelib.pydeclarativelib import write_text_atomic
import datetime
import json
import uuid

@dataclass
class ManifestState:
    run_id : str
    repo_dir: str
    upstream_hash: str
    datetime: str
    hash : str
    manifest_path : str | None

    def to_json(self) -> str:

        return json.dumps({
            "run_id": self.run_id,
            "repo_dir": self.repo_dir,
            "upstream_hash": self.upstream_hash,
            "datetime": self.datetime,
            "hash": self.hash,
            "manifest_path": self.manifest_path,
        })


def run(parent_id : str | None = None):
    run_id = "odin-manifest-" + str(uuid.uuid4())
    logger: Logger = setup_logging(level="INFO", appName="odin_generate_manifest") 
    logger = WithContext(logger, {"run_id": run_id})
    do_anyway = False
    try:
        if parent_id is not None:
            logger = WithContext(logger, {"parent_id": parent_id})
        
        odin_config: OdinConfig = load_config()
        state_path = odin_config.manifest_dir / odin_config.manifest_state_name
        manifest_path = odin_config.manifest_dir / odin_config.manifest_file_name
        upstream_statepath = odin_config.manifest_job.upstream_statepath
        previous_signature = None
        tracker = Tracker()
        utc_timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat()
        is_statefile_exists = True
        is_upstream_statefile_exists = True
        upstream_hash = "A"
        previous_upstream_hash = "B"

        if not state_path.exists():
            logger.info(f"existing statefile not found at {state_path}")
            is_statefile_exists = False
        if not upstream_statepath.exists():
            logger.info(f"existing upstream statefile not found at {upstream_statepath}")
            is_upstream_statefile_exists = False
        
        if is_statefile_exists:
            previous_signature = get_hash(statefile_path=state_path)
            previous_upstream_hash = get_upstream_hash(statefile_path=state_path)
            tracker.set_signature_data(run_id=run_id,column=RunSignature.PREVIOUS_UPSTREAM_SIGNATURE, signature_data=previous_upstream_hash)
            tracker.set_signature_data(run_id=run_id,column=RunSignature.PREVIOUS_JOB_SIGNATURE, signature_data=previous_signature)
        
        if is_upstream_statefile_exists:
            upstream_hash = get_hash(statefile_path=upstream_statepath)
            tracker.set_signature_data(run_id=run_id,column=RunSignature.CURRENT_UPSTREAM_SIGNATURE, signature_data=upstream_hash)

        def decide_whether_to_do_this_job() -> JobState:
            if not is_statefile_exists:
                logger.info(f"existing statefile not found at {state_path}")
                return JobState.STATE_NOT_FOUND_SHOULD_UPDATE
            if not is_upstream_statefile_exists:
                logger.info(f"existing upstream statefile not found at {upstream_statepath}")
                return JobState.STATE_NOT_FOUND_SHOULD_UPDATE
            
            logger.info(f"upstream_hash:{upstream_hash}; previous_upstream_hash:{previous_upstream_hash}")

            if upstream_hash == previous_upstream_hash:
                return JobState.STILL_CURRENT_SHOULD_SKIP
            else:
                return JobState.STATE_EXPIRED


        def write_the_manifest(manifest_path: Path):
            logger.info("starting to generate manifest")
            write_manifest(
                                root_dir=odin_config.repo_dir, 
                                manifest_path = manifest_path, 
                                format_type="yaml", 
                                exclude_patterns=odin_config.manifest_exclusions
                            )
            logger.info(f"generated manifest at {manifest_path}")
        

        def generate_manifest_state_file(manifest_hash : str, 
                                        upstream_hash : str, 
                                        manifest_path : Path):
            manifest_state : ManifestState = ManifestState(
                            run_id=run_id,
                            repo_dir= str(odin_config.repo_dir),
                            upstream_hash=upstream_hash,
                            datetime=utc_timestamp,
                            hash=manifest_hash,
                            manifest_path=str(manifest_path))
            
            state_text = manifest_state.to_json()
            write_text_atomic(at = state_path, the_text=state_text)
            logger.info(f"odin manifest job completed successfully; output at {manifest_path}")

        tracker.start_run(run_id=run_id,
                    run_name="odin_tarball",
                    meta={
                        "timestamp" :  utc_timestamp
                    })
        job_state = decide_whether_to_do_this_job()
        
        if not do_anyway and job_state == JobState.STILL_CURRENT_SHOULD_SKIP:
            logger.info("no upstream changes; should skip")
            tracker.finish_run(run_id=run_id, status=Tracker.JobTrackingStatus.SKIPPED)
            return {"success" : True}
        
        write_the_manifest(manifest_path=manifest_path)
        manifest_hash: str = sha256_file(path = manifest_path)
        tracker.set_signature_data(run_id=run_id,column=RunSignature.JOB_RESULT_SIGNATURE, signature_data=manifest_hash)
        generate_manifest_state_file(manifest_hash=manifest_hash, 
                                        upstream_hash=upstream_hash, 
                                        manifest_path=manifest_path)
        logger.info(BackupJobResult.SUCCESS)
        tracker.finish_run(run_id=run_id, status=str(Tracker.JobTrackingStatus.SUCCESS))
    except:
        logger.exception("did not generate odin manifest")
        raise



if __name__ == "__main__":
    run()