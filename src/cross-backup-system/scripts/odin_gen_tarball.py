#!/usr/bin/env python3
"""
odin_backup_job.py

Create a timestamped backup folder containing:
  1) tar.gz of the Odin repo (with config-driven exclude patterns)
  2) a manifest file copied from a configurable location
  3) a sha256 checksum file for the tarball (sha256sum format)

Idempotence:
  - Implement `should_skip_run()` to check prior job state.
"""

from __future__ import annotations
import argparse
import hashlib
import os
from pathlib import Path
import shutil
import sys
import tarfile
from datetime import datetime
from fnmatch import fnmatch
import yaml
import tempfile
from backuplib.checksumtools import sha256_string, hash_script
from backuplib.exceptions import ConfigException
from backuplib.logging import setup_logging, WithContext
from backuplib.configloader import OdinConfig, load_config
from backuplib.jobstatehelper import load_manifest_state, manifest_state_output_sig, \
        read_manifest_hash
from backuplib.filesutil import atomic_write_text
from dataclasses import dataclass, asdict
from typing import Dict, List, Optional
import json

# ----------------------------
# Config model (YAML expected)
# ----------------------------
# Example YAML:
# repo_dir: "/home/adam/Odin"
# backups_dir: "/home/adam/OdinBackups"
# manifest_path: "/home/adam/Manifests/odin_manifest.json"
# exclude_patterns:
#   - ".git/**"
#   - ".venv/**"
#   - "**/__pycache__/**"
#   - "*.tmp"
# datetime_format: "%Y%m%d_%H%M%S"   # optional; default as below
# tarball_name: "OdinRepo.tar.gz"    # optional; default as below

DEFAULT_DT_FORMAT = "%Y%m%d_%H%M%S"
DEFAULT_TARBALL_NAME = "Odin.tar.gz"


CONFIG_PATH = Path("~/.config/odin/odin_config.yaml").expanduser()

def load_config() -> dict :

    with open(CONFIG_PATH, "r") as f:
        CONFIG = yaml.safe_load(f)
    try:
        config = {
            "repo_dir" : CONFIG["REPO_DIR"],
            "manifest_dir": CONFIG["MANIFEST_DIR"],
            "exclusions": CONFIG["TARBALL_EXCLUDES"],
            "local_zone": CONFIG["LOCAL_ZONE"],
            "output_path": CONFIG["TARBALL_OUPUT"],
            "manifest_filename": CONFIG["MANIFEST_FILENAME"],
            "tarball_name": CONFIG["DEFAULT_TARBALL_NAME"]
        }
    except KeyError as e:
        raise ConfigException(f"Missing required config key. Config path: {CONFIG_PATH}.")
odinConfig: OdinConfig = load_config()



logger = setup_logging(level="INFO", appName="odin_generate_tarball") 

# -------------------------
# Data types
# -------------------------

@dataclass
class InputsFingerprint:
    manifest_hash: str
    script_hash: Optional[str] = None
    config_hash: Optional[str] = None

@dataclass
class OutputsInfo:
    tarball_path: str
    tarball_sha256: str
    size_bytes: int
    created_at: str

@dataclass
class Policy:
    rebuild_if_missing: bool = True
    sensitivity: List[str] = None  # which fields in InputsFingerprint trigger rebuild when changed

    def __post_init__(self):
        if self.sensitivity is None:
            self.sensitivity = ["manifest_hash", "script_hash", "config_hash"]

@dataclass
class Provenance:
    upstream_state_path: Optional[str] = None
    manifest_state_ts: Optional[str] = None

@dataclass
class TarballJobState:
    version: int
    inputs: InputsFingerprint
    outputs: Optional[OutputsInfo]
    policy: Policy
    provenance: Provenance

    @staticmethod
    def load(path: Path) -> Optional["TarballJobState"]:
        if not path.exists():
            return None
        raw = json.loads(path.read_text(encoding="utf-8"))
        return TarballJobState(
            version=raw.get("version", 1),
            inputs=InputsFingerprint(**raw["inputs"]),
            outputs=OutputsInfo(**raw["outputs"]) if raw.get("outputs") else None,
            policy=Policy(**raw.get("policy", {})),
            provenance=Provenance(**raw.get("provenance", {})),
        )

    def save(self, path: Path) -> None:
        payload = {
            "version": self.version,
            "inputs": asdict(self.inputs),
            "outputs": asdict(self.outputs) if self.outputs else None,
            "policy": asdict(self.policy),
            "provenance": asdict(self.provenance),
        }
        atomic_write_text(path, json.dumps(payload, indent=2, sort_keys=True))



def build_new_state(
    upstream_state_path: Path,
    expected_tarball_path: Path,
    manifest_hash_key: str,
    script_path: Path,
    #config_paths: Optional[List[Path]],
    prior_policy: Optional[Policy] = None,
) -> TarballJobState:
    up = read_manifest_hash(upstream_state_path, manifest_hash_key=manifest_hash_key)
    script_h = hash_script(script_path)
    #config_h = hash_config(config_paths) if config_paths else None

    inputs = InputsFingerprint(
        manifest_hash=up["manifest_hash"],
        script_hash=script_h,
    #    config_hash=config_h,
    )

    # compute outputs from the tarball we just built
    tar_sha = sha256_file(expected_tarball_path)
    size = expected_tarball_path.stat().st_size
    created_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    outputs = OutputsInfo(
        tarball_path=str(expected_tarball_path),
        tarball_sha256=tar_sha,
        size_bytes=size,
        created_at=created_at,
    )

    policy = prior_policy or Policy()
    provenance = Provenance(
        upstream_state_path=str(upstream_state_path),
        manifest_state_ts=up.get("manifest_state_ts"),
    )

    return TarballJobState(
        version=1,
        inputs=inputs,
        outputs=outputs,
        policy=policy,
        provenance=provenance,
    )

# ----------------------------
# Helpers
# ----------------------------

def load_prior_step_state():
    manifest_state = load_manifest_state()
    pass

@dataclass
class ShouldRunResult:
    should_run: bool
    reason: str

def decide_should_run(
    current_inputs: InputsFingerprint,
    prior: Optional[TarballJobState],
    expected_tarball_path: Path,
) -> ShouldRunResult:
    # no prior state -> run
    if prior is None:
        return ShouldRunResult(True, "no prior state")

    # compare sensitive inputs
    sens = set(prior.policy.sensitivity)
    for field in ["manifest_hash", "script_hash", "config_hash"]:
        if field in sens:
            cur = getattr(current_inputs, field)
            prev = getattr(prior.inputs, field)
            if cur != prev:
                return ShouldRunResult(True, f"{field} changed")

    # ensure tarball exists (or rebuild if policy allows)
    if not expected_tarball_path.exists():
        if prior.policy.rebuild_if_missing:
            return ShouldRunResult(True, "tarball missing; rebuild_if_missing=True")
        else:
            return ShouldRunResult(False, "tarball missing; rebuild_if_missing=False")

    # verify integrity matches recorded hash if we have one
    if prior.outputs and prior.outputs.tarball_sha256:
        current_hash = sha256_file(expected_tarball_path)
        if current_hash != prior.outputs.tarball_sha256:
            return ShouldRunResult(True, "tarball hash mismatch with recorded outputs")

    return ShouldRunResult(False, "inputs unchanged and tarball verified")


def should_skip_run(config: dict) -> bool:
    """
    TODO: Implement your idempotence policy here.
    For example, consult prior job outputs or a state DB written by the previous job.
    Return True if no work is needed; False otherwise.
    """
    return False  # placeholder


def resolve_paths(config: dict) -> dict:
    paths = {
        "repo_dir": Path(config["repo_dir"]).expanduser().resolve(),
        "backups_dir": Path(config["output_path"]).expanduser().resolve(),
        "manifest_dir": Path(config["manifest_dir"]).expanduser().resolve(),
    }

    paths["manifest_path"] = config["manifest_dir"] / config["manifest_filename"]

    if not paths["repo_dir"].is_dir():
        raise FileNotFoundError(f"repo_dir not found: {paths["repo_dir"]}")
    if not paths["manifest_path"].is_file():
        raise FileNotFoundError(f"manifest_path not found: {paths["manifest_path"]}")

    #backups_dir.mkdir(parents=True, exist_ok=True)
    return paths


def make_backup_folder(backups_dir: Path, dt_format: str) -> Path:
    stamp = datetime.now().strftime(dt_format)
    folder = backups_dir / f"OdinBackup_{stamp}"
    folder.mkdir(parents=False, exist_ok=False)
    return folder


def path_matches_any(rel: str, patterns: list[str]) -> bool:
    """Match POSIX-style relative path against glob patterns (supports **)."""
    return any(fnmatch(rel, pat) for pat in patterns)


def iter_repo_files(repo_dir: Path, exclude_patterns: list[str]) -> list[Path]:
    """Yield files (and symlinks) to include, honoring excludes."""
    files = []
    for p in repo_dir.rglob("*"):
        rel = p.relative_to(repo_dir).as_posix()
        if exclude_patterns and path_matches_any(rel, exclude_patterns):
            continue
        # Optionally skip directories here; tar.add will need files and dirs
        files.append(p)
    return files


def create_tarball(
                repo_dir: Path, 
                exclude_patterns: list[str], 
                dest_tar_gz: Path) -> None:
    """
    Create tar.gz at dest_tar_gz containing repo_dir contents (without nesting under a top folder).
    Excludes are matched against POSIX-style relative paths.
    """
    logger.info(f"Creating tarball: {dest_tar_gz}")
    with tarfile.open(dest_tar_gz, "w:gz") as tar:
        for p in repo_dir.rglob("*"):
            rel = p.relative_to(repo_dir).as_posix()
            if exclude_patterns and path_matches_any(rel, exclude_patterns):
                continue
            # Preserve directory structure but avoid including the repo root name.
            tar.add(p, arcname=rel)


def sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def write_sha256sum_file(target_file: Path, checksum_out: Path) -> None:
    """
    Write in standard `sha256sum` format: "<hash>  <filename>"
    filename should be shown as base name (typical sha256sum behavior).
    """
    digest = sha256_file(target_file)
    line = f"{digest}  {target_file.name}\n"
    checksum_out.write_text(line, encoding="utf-8")

def generate_state_file(
                            config: dict,
                            tar_path: Path, 
                            checksum_path: Path
                        ):
    manifest_state_out_sig = manifest_state_output_sig()

    state = {
        "init_sig" : manifest_state_out_sig

    }

    return state

# ----------------------------
# Main operation
# ----------------------------

def run_job(config_path: Path, dry_run: bool = False) -> Path:
    cfg = load_config()
    
    if should_skip_run(cfg):
        logger.info("No changes detected by prior job; skipping backup.")
        # You could raise SystemExit(0) here if you prefer.
        return Path("-skipped-")

    paths = resolve_paths(cfg)
    #dt_format = cfg.get("datetime_format", DEFAULT_DT_FORMAT)
    #tarball_name = cfg.get("tarball_name", DEFAULT_TARBALL_NAME)

    exclude_patterns = cfg.get("exclude_patterns", []) or []
    tarball_name = cfg["tarball_name"]
    backup_folder = paths["backups_dir"]
    manifest_path = paths["manifest_path"]

    tar_path = backup_folder / tarball_name
    checksum_path = backup_folder / f"{tarball_name}.sha256"
    manifest_dest = backup_folder / Path(manifest_path.name)

    logger.info("Backup folder: %s", backup_folder)
    logger.info("Excluding patterns: %s", exclude_patterns if exclude_patterns else "(none)")

    if dry_run:
        logger.info("[DRY RUN] Would create tarball at: %s", tar_path)
        logger.info("[DRY RUN] Would copy manifest to: %s", manifest_dest)
        logger.info("[DRY RUN] Would write checksum to: %s", checksum_path)
        return backup_folder

    with tempfile.TemporaryDirectory() as tmpdir:
        # Create tarball
        create_tarball(paths["repo_dir"], exclude_patterns, tmpdir)
        os.replace(tmpdir, tar_path)

    # Copy manifest
    shutil.copy2(manifest_path, manifest_dest)

    # Write checksum (sha256sum format)
    write_sha256sum_file(tar_path, checksum_path)

    logger.info("Backup complete.")
    return backup_folder


def parse_args(argv: list[str]) -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Odin backup job: tar repo + manifest + checksum.")
    ap.add_argument("-c", "--config", required=True, type=Path, help="Path to YAML config.")
    ap.add_argument("--dry-run", action="store_true", help="Plan actions without writing outputs.")
    ap.add_argument("-v", "--verbose", action="count", default=0, help="Increase verbosity (-v, -vv).")
    return ap.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    try:
        run_job(args.config, dry_run=args.dry_run)
    except Exception as e:
        logger.exception("exception: {e}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
