from dataclasses import dataclass
from pathlib import Path
from backuplib.exceptions import ConfigException
import yaml

CONFIG_PATH = Path("~/.config/odin/odin_config.yaml").expanduser()

@dataclass
class QuickManifestConfig:
    manifest_exclusions : list[str]
    dir : Path
    statefile_name : str
    upstream_statepath : Path
    outfile : Path

@dataclass
class EncryptionJob:
    upstream_statepath : Path
    dir : Path
    statefile_name : str

@dataclass
class ManifestJob:
    upstream_statepath: Path
    dir: Path
    statefile_name: str

@dataclass
class RsyncMirroringJob:
    upstream_statepath : Path

@dataclass
class OdinConfig:
    repo_dir : Path
    manifest_exclusions : list[str]
    local_zone: str
    default_tarball_name : str
    manifest_state_name: str
    tarball_exclusions: list[str]
    manifest_dir: Path
    manifest_file_name: str
    tarball_dir: Path
    latest_tarball_filename : str
    recipient: str
    encrypted_dir: Path
    offsite_sync_dir : Path
    tarball_dir_idempotent: Path
    tarball_state_filename : str
    git_pull_statefile_name : str
    tarball_job_upstream_statepath : Path
    encrypted_tarball_name: str
    quick_manifest_config : QuickManifestConfig
    encryption_job : EncryptionJob
    rsync_mirroring : list[str]
    sidecar_root : str
    rsync_exclusions_file : Path
    rsync_mirroring_file : str
    rsync_mirroring_job : RsyncMirroringJob
    manifest_job : ManifestJob


def load_config() -> OdinConfig:
    with open(CONFIG_PATH, "r") as f:
        config = yaml.safe_load(f)
    try:
        repo_dir = Path(config["REPO_DIR"]).expanduser().resolve()
        manifest_dir = Path(config["MANIFEST_DIR"]).expanduser().resolve()
        odinConfig = OdinConfig(
                    repo_dir=repo_dir,
                    manifest_exclusions = config["MANIFEST_EXCLUSIONS"],
                    local_zone = config["LOCAL_ZONE"],
                    default_tarball_name = config["DEFAULT_TARBALL_NAME"],
                    manifest_state_name = config["MANIFEST_STATE_NAME"],
                    tarball_exclusions = config["TARBALL_EXCLUSIONS"],
                    manifest_dir = manifest_dir,
                    manifest_file_name=config["MANIFEST_FILENAME"],
                    tarball_dir = Path(config["TARBALL_OUPUT"]),
                    latest_tarball_filename = config["LATEST_TARBALL_FILENAME"],
                    recipient = config["RECIPIENT"],
                    encrypted_dir= Path(config["ODIN_ENCRYPTED_DIR"]),
                    offsite_sync_dir = Path(config["OFFSITE_SYNC_DIR"]),
                    tarball_dir_idempotent = Path(config["TARBALL_OUTPUT_IDEMPOTENT"]),
                    tarball_state_filename=config["TARBALL_STATE_FILENAME"],
                    git_pull_statefile_name=config["GIT_PULL_STATEFILE_NAME"],
                    tarball_job_upstream_statepath=Path(config["TARBALL_JOB_UPSTREAM_STATEPATH"]),
                    encrypted_tarball_name=config["ENCRYPTED_TARBALL_NAME"],
                    quick_manifest_config = QuickManifestConfig(
                        dir = Path(config["QUICK_MANIFEST_SCAN"]["dir"]),
                        statefile_name = Path(config["QUICK_MANIFEST_SCAN"]["statefile_name"]),
                        upstream_statepath = Path(config["QUICK_MANIFEST_SCAN"]["upstream_statepath"]),
                        outfile = Path(config["QUICK_MANIFEST_SCAN"]["outfile"]),
                        manifest_exclusions = config["MANIFEST_EXCLUSIONS"]
                    ),
                    encryption_job = EncryptionJob(
                        upstream_statepath = Path(config["ENCRYPTION_JOB"]["upstream_statepath"]),
                        dir = Path(config["ENCRYPTION_JOB"]["dir"]),
                        statefile_name = Path(config["ENCRYPTION_JOB"]["statefile_name"])
                    ),
                    rsync_mirroring=config["RSYNC_MIRRORING"],
                    sidecar_root=config["SIDECAR_ROOT"],
                    rsync_exclusions_file=Path(config["RSYNC_EXCLUSIONS_FILE"]),
                    rsync_mirroring_file= config["RSYNC_MIRRORING_FILE"],
                    rsync_mirroring_job = RsyncMirroringJob(
                        upstream_statepath=Path(config["RSYNC_MIRRORING_JOB"]["UPSTREAM_STATEPATH"])
                    ),
                    manifest_job = ManifestJob(upstream_statepath=Path(config["MANIFEST_JOB"]["upstream_statepath"]),
                                                dir=Path(config["MANIFEST_JOB"]["dir"]),
                                                statefile_name=config["MANIFEST_JOB"]["statefile_name"]
                                               )
                )
        return odinConfig
    except KeyError as e:
        raise ConfigException(f"Missing required config key. Config path: {CONFIG_PATH}.")
