import uuid
from backuplib.audit import Tracker
import scripts.odin_pull_git_updates
import scripts.odin_quick_manifest_job
import scripts.run_mirror_to_samson
import scripts.odin_to_tarball_base
import scripts.encrypt_tarball_base
from pydeclarativelib.declarativeaudit import audited_by
from backuplib.logging import setup_logging, Logger
from backuplib.configloader import load_config, OdinConfig
import datetime
from zoneinfo import ZoneInfo

def run():

    tracker = Tracker()
    run_id = "odin-full-backup-"+str(uuid.uuid4())
    odin_cfg: OdinConfig = load_config()
    logger : Logger = setup_logging(appName = "odin-gen-tarball-backup")
    tz = ZoneInfo(odin_cfg.local_zone)
    datestamp = datetime.datetime.now(tz).strftime("%Y_%m_%d-%H-%M")

    @audited_by(tracker, with_step_name="make idempotent copy", and_run_id = run_id)
    def odin_pull_job(parent_id: str):
        return scripts.odin_pull_git_updates.run(parent_id=parent_id)


    tracker.start_run(run_id=run_id,
                    run_name="odin_tarball",
                    meta={
                        "timestamp" :  utc_timestamp,
                        "timezone" : odin_cfg.local_zone
                    })



    


    
    scripts.odin_quick_manifest_job.run(parent_id=parent_id)
    scripts.run_mirror_to_samson.run(parent_id=parent_id)
    scripts.odin_to_tarball_base.run(parent_id=parent_id)
    scripts.encrypt_tarball_base.run(parent_id=parent_id)
