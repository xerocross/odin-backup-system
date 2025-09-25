from __future__ import annotations

"""
Refactor pass 1: flatter flow, cleaner state handling, idempotent guard.

External deps (assumed available from backuplib):
- write_manifest(root_dir, manifest_path, format_type, exclude_patterns)
- Tracker (audit)
- quick_scan_signature(root, exclude) -> QuickManifestSig (dataclass-like)
- atomic_write_text(path: Path, text: str)
- compute_sha256(path: Path) -> str
- load_config() -> OdinConfig with fields used below
- setup_logging(level, appName), WithContext(logger, context_dict)

Behavior:
- Compute a quick signature of repo (cheap hash over file names/mtimes/etc.).
- Load previous manifest state if present.
- Decide: SKIP vs REBUILD by comparing init quick signature + last output hash vs current.
- If REBUILD/NOT_FOUND/FAILED: write fresh manifest, then write state json.
- Always record audit steps.

Idempotency logic:
- If the quick-signature (input fingerprint) matches the one in state AND
  the manifest file exists AND its hash matches the recorded output hash,
  then we skip.
- Otherwise, we rebuild.
"""

from dataclasses import dataclass, asdict
from enum import Enum
from pathlib import Path
from typing import Optional
import contextvars
import json
import logging
import os
import tempfile
import uuid
from datetime import datetime
from zoneinfo import ZoneInfo

from backuplib.generate_manifest import write_manifest
from backuplib.audit import Tracker
from backuplib.filesutil import (
    quick_scan_signature,
    atomic_write_text,
    QuickManifestSig,
)
from backuplib.checksumtools import compute_sha256, digest
from backuplib.configloader import OdinConfig, load_config
from backuplib.logging import setup_logging, WithContext

# -----------------------------------------------------------------------------
# Context & logging
# -----------------------------------------------------------------------------
current_tracker = contextvars.ContextVar("current_tracker", default=None)
current_run_id = contextvars.ContextVar("current_run_id", default=None)

LOG = setup_logging(level="INFO", appName="odin_generate_manifest")

# -----------------------------------------------------------------------------
# Data models
# -----------------------------------------------------------------------------
class JobState(Enum):
    STATE_EXPIRED = 1
    STATE_NOT_FOUND_SHOULD_UPDATE = 2
    STILL_CURRENT_SHOULD_SKIP = 3
    STATE_CHECK_FAILED_SHOULD_REBUILD = 4


def _now_tz(tz: ZoneInfo) -> str:
    return datetime.now(tz).strftime("%Y-%m-%d_%H:%M:%S")


@dataclass
class ManifestState:
    init_signature_hash: str
    output_signature_hash: str

    @staticmethod
    def from_json_text(text: str) -> "ManifestState":
        obj = json.loads(text)
        # Back-compat for older keys
        init_hash = obj.get("initial_signature_hash") or obj.get("init_sig_hex")
        out_hash = obj.get("output_signature_hash") or obj.get("output_sig_hex")
        return ManifestState(
            init_signature_hash=init_hash,
            output_signature_hash=out_hash,
        )

    def to_json(self) -> str:
        return json.dumps(
            {
                "initial_signature_hash": self.init_signature_hash,
                "output_signature_hash": self.output_signature_hash,
            },
            sort_keys=True,
            indent=2,
        )


@dataclass
class ManifestInfo:
    run_id: str
    repo_dir: Path
    manifest_path: Path
    state_path: Path
    init_qsig: QuickManifestSig
    initial_signature_hash: str
    current_manifest_sig: Optional[str]
    local_zone: ZoneInfo


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------

def perform_initial_job_setup(odin_cfg: OdinConfig, 
                              *, run_id: str, 
                              logger: logging.Logger) -> ManifestInfo:
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


def load_state_if_any(
                        state_path: Path, 
                        *, 
                        logger: logging.Logger) -> Optional[ManifestState]:
    if not state_path.exists():
        logger.debug("no state file at %s", state_path)
        return None
    try:
        text = state_path.read_text(encoding="utf-8")
        return ManifestState.from_json_text(text)
    except Exception:
        logger.exception("failed to read/parse state file; will rebuild")
        return None


def decide_job_state(
                            manifestInfo: ManifestInfo, 
                            manifestState: Optional[ManifestState], 
                            *, 
                            logger: logging.Logger
                     ) -> JobState:
    # No manifest or no state? Build.
    if not manifestInfo.manifest_path.exists() or manifestState is None:
        logger.info("found no existing manifest or state; will build")
        return JobState.STATE_NOT_FOUND_SHOULD_UPDATE

    try:
        logger.debug("state.init=%s, quick=%s", manifestState.init_signature_hash, manifestInfo.initial_signature_hash)
        
        if manifestState.init_signature_hash == manifestInfo.initial_signature_hash:
            prev_out = manifestState.output_signature_hash
            cur_out = manifestInfo.current_manifest_sig
            logger.info("previous manifest hash=%s; current manifest hash=%s", prev_out, cur_out)
            if cur_out is not None and prev_out == cur_out:
                logger.info("existing state matches current; skipping")
                return JobState.STILL_CURRENT_SHOULD_SKIP
            else:
                logger.info("manifest hash mismatch or missing; will rebuild")
                return JobState.STATE_EXPIRED
        else:
            logger.info("input signature changed; will rebuild")
            return JobState.STATE_EXPIRED
    except Exception:
        logger.exception("error while checking state; will rebuild")
        return JobState.STATE_CHECK_FAILED_SHOULD_REBUILD


def write_manifest_and_state(
    odin_cfg: OdinConfig,
    mi: ManifestInfo,
    *,
    tracker: Tracker,
    logger: logging.Logger,
) -> Path:
    # Step 1: generate manifest to temp and atomically move
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir) / "manifest.yaml"
        with tracker.record_step(mi.run_id, "generating odin manifest") as rec:
            try:
                logger.info("generating manifest -> %s", tmp_path)
                write_manifest(
                    root_dir=mi.repo_dir,
                    manifest_path=str(tmp_path),
                    format_type="yaml",
                    exclude_patterns=odin_cfg.manifest_exclusions,
                )
                os.replace(tmp_path, mi.manifest_path)
                rec["status"] = "success"
            except Exception as e:
                logger.exception("generating odin manifest failed: %s", e)
                rec["status"] = "failed"
                tracker.finish_run(mi.run_id, "failed", output_path=str(mi.manifest_path))
                raise

    # Step 2: compute output hash & write state file
    with tracker.record_step(mi.run_id, "writing manifest state") as rec:
        try:
            out_sha = compute_sha256(mi.manifest_path)
            state = ManifestState(
                init_signature_hash=mi.initial_signature_hash,
                output_signature_hash=out_sha,
            )
            atomic_write_text(mi.state_path, state.to_json())
            rec["status"] = "success"
            logger.info("odin manifest job completed successfully; output at %s", mi.manifest_path)
            tracker.finish_run(
                mi.run_id,
                "success",
                output_path=str(mi.manifest_path),
                output_sig_hash=state.output_signature_hash,
            )
        except Exception:
            rec["status"] = "failed"
            tracker.finish_run(mi.run_id, "failed", output_path=str(mi.manifest_path))
            raise

    return mi.manifest_path


# -----------------------------------------------------------------------------
# Orchestration
# -----------------------------------------------------------------------------

def odin_run_manifest_job() -> None:
    run_id = f"manifest-{uuid.uuid4()}"
    logger = WithContext(LOG, {"log_id": run_id})

    try:
        odin_cfg: OdinConfig = load_config()
        tracker = Tracker()
        tracker.start_run(
            run_id=run_id,
            run_name="generate_manifest",
            meta={"job": "generate manifest"},
        )

        mi = perform_initial_job_setup(odin_cfg, run_id=run_id, logger=logger)
        st = load_state_if_any(mi.state_path, logger=logger)
        decision = decide_job_state(mi, st, logger=logger)

        if decision == JobState.STILL_CURRENT_SHOULD_SKIP:
            logger.info("skipping build: manifest is current")
            tracker.finish_run(run_id, "success", output_path=str(mi.manifest_path), output_sig_hash=st.output_signature_hash if st else None)
            return

        # For all other cases: (re)build
        write_manifest_and_state(odin_cfg, mi, tracker=tracker, logger=logger)

    except Exception as e:
        logger.exception("manifest job failed: %s", e)


def run() -> None:
    odin_run_manifest_job()


if __name__ == "__main__":
    run()
