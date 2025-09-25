
import uuid
from backuplib.configloader import OdinConfig, load_config
from backuplib.logging import setup_logging, WithContext, Logger


odinConfig: OdinConfig = load_config()
run_id = "manifest-job-" + str(uuid.uuid4())
logger : Logger = setup_logging(appName="odin_generate_manifest")

def run():
    pass





if __name__ == "__main__":
    run()