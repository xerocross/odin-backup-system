import uuid
from backuplib.audit import Tracker
import scripts.odin_pull_git_updates
import scripts.odin_quick_manifest_job
import scripts.run_mirror_to_samson
import scripts.odin_to_tarball_base
import scripts.encrypt_tarball_base
from pydeclarativelib.declarativeaudit import audited_by
from backuplib.logging import setup_logging, Logger, WithContext
from backuplib.configloader import load_config, OdinConfig
from pydeclarativelib.declarativesuccess import with_try_except_and_trace
import datetime
from zoneinfo import ZoneInfo
from typing import List


class JobFailureException(Exception):
    """A Job failed"""
    pass

def run():
    tracker = Tracker()
    run_id = "odin-full-backup-"+str(uuid.uuid4())
    odin_cfg: OdinConfig = load_config()
    logger : Logger = setup_logging(appName = "odin-full-backup")
    logger = WithContext(logger, {"run_id": run_id}) # type: ignore
    utc_timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat()
    tz = ZoneInfo(odin_cfg.local_zone)
    trace : List[str] = [f"run_id:{run_id}"]
    parent_id = run_id

    @audited_by(tracker, with_step_name="odin_pull", and_run_id = run_id)
    @with_try_except_and_trace(if_success_then_message=None, if_failed_then_message=None, with_trace=trace)
    def odin_pull_job(parent_id: str):
        return scripts.odin_pull_git_updates.run(parent_id=parent_id)
    
    @audited_by(tracker, with_step_name="quick_manifest_job", and_run_id = run_id)
    @with_try_except_and_trace(if_success_then_message=None, if_failed_then_message=None, with_trace=trace)
    def run_quick_manifest_job(parent_id: str):
        return scripts.odin_quick_manifest_job.run(parent_id=parent_id)

    @audited_by(tracker, with_step_name="mirror to samson", and_run_id = run_id)
    @with_try_except_and_trace(if_success_then_message=None, if_failed_then_message=None, with_trace=trace)
    def run_mirror_to_samson(parent_id:str):
        return scripts.run_mirror_to_samson.run(parent_id=parent_id)

    @audited_by(tracker, with_step_name="odin to tarball", and_run_id = run_id)
    @with_try_except_and_trace(if_success_then_message=None, if_failed_then_message=None, with_trace=trace)
    def run_odin_to_tarball(parent_id:str):
        return scripts.odin_to_tarball_base.run(parent_id=parent_id)

    @audited_by(tracker, with_step_name="encrypt tarball", and_run_id = run_id)
    @with_try_except_and_trace(if_success_then_message=None, if_failed_then_message=None, with_trace=trace)
    def run_encryp_tarball(parent_id:str):
        return scripts.encrypt_tarball_base.run(parent_id=parent_id)

    tracker.start_run(run_id=run_id,
                    run_name="odin_full_backup",
                    meta={
                        "timestamp" :  utc_timestamp,
                        "timezone" : odin_cfg.local_zone
                    })

    try:
        res = odin_pull_job(parent_id=parent_id)
        if not res["success"]:
            raise JobFailureException()
        res = run_quick_manifest_job(parent_id=parent_id)
        if not res["success"]:
            raise JobFailureException()
        res = run_mirror_to_samson(parent_id=parent_id)
        if not res["success"]:
            raise JobFailureException()
        res = run_odin_to_tarball(parent_id=parent_id)
        if not res["success"]:
            raise JobFailureException()
        res = run_encryp_tarball(parent_id=parent_id)
        if not res["success"]:
            raise JobFailureException()
        tracker.finish_run(run_id, "success")
        logger.info("odin full backup succeeded")
    except:
        logger.exception("odin full backup failed")
        tracker.finish_run(run_id, "failed")
    
    
if __name__ == "__main__":
    run()
