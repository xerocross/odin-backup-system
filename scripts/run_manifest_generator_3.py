from typing import Callable, Iterable, Tuple, TypeVar, Generic, List, FunctionType, NamedTuple, Dict
from backuplib.logging import setup_logging, WithContext
from logging import Logger
from backuplib.decisionengine import ProgramLogicRunner, DecisionSignal
from backuplib.configloader import OdinConfig, load_config
from backuplib.checksumtools import compute_sha256, digest
import os

LOG = setup_logging(level="INFO", appName="odin_generate_manifest")
context : Dict = {}
odin_cfg = load_config()


programLogicRunner = ProgramLogicRunner(context)

Context = TypeVar("Context")



@programLogicRunner.action(signal = DecisionSignal.LOAD_PREVIOUS_RUN_STATE)
def _load_previous_state(context: Context):
    pass


@programLogicRunner.action(signal = DecisionSignal.LOAD_PREVIOUS_OUTPUT)
def _load_previous_output(context):
    manifest_path = odin_cfg.manifest_dir / odin_cfg.manifest_file_name
    if os.path.exists(manifest_path):
        context.output_loaded = True
    else:
        context.output_loaded = False
        context.is_previous_run = False





def perform_initial_job_setup(odin_cfg: OdinConfig, 
                              *, run_id: str, 
                              logger: Logger) -> Context:
    logger.info("starting the odin manifest job run_id=%s", run_id)
    odin_cfg.manifest_dir.mkdir(parents=True, exist_ok=True)

    manifest_path = odin_cfg.manifest_dir / odin_cfg.manifest_file_name
    state_path = odin_cfg.manifest_dir / odin_cfg.manifest_state_name

    qsig: QuickManifestSig = quick_scan_signature(
        root=odin_cfg.repo_dir, exclude=odin_cfg.manifest_exclusions
    )
    initial_signature_hash = digest(asdict(qsig))
    
    current_manifest_sig = compute_sha256(manifest_path) if manifest_path.exists() else None

    return ManifestInfo(
        run_id=run_id,
        repo_dir=odin_cfg.repo_dir,
        manifest_path=manifest_path,
        state_path=state_path,
        init_qsig=qsig,
        initial_signature_hash=initial_signature_hash,
        current_manifest_sig=current_manifest_sig,
        local_zone=ZoneInfo(getattr(odin_cfg, "local_zone", "UTC")),
    )




