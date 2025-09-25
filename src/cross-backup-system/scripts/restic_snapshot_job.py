#!/usr/bin/env python3

import json
import os
import shlex
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import List, Optional
from backuplib.configloader import load_config, OdinConfig
from backuplib.logging import setup_logging, Logger, WithContext
import uuid

run_id = "odin-resic-job-" + str(uuid.uuid4())

odin_config: OdinConfig = load_config()
logger: Logger = setup_logging(appName="restic_backup")
logger = WithContext(logger, {"run_id": run_id})

# ---- USER SETTINGS (edit these) ----
TARGET_PATH = odin_config.restic_dropbox_job.target
PASSWORD_FILE = odin_config.restic_dropbox_job.password_file_path


# What to back up (dirs or files)
SOURCES = [
    str(odin_config.repo_dir),        # example
]

# Exclusions (use a file if you prefer)
EXCLUDE_FILE = odin_config.repo_dir / "config" / "restic-excludes"  # optional

# Retention policy (GFS-style)
FORGET_ARGS = [
    "--keep-daily", "14",
    "--keep-weekly", "20",
    "--keep-monthly", "120",
    "--keep-yearly", "100",
    "--prune",
]

# Integrity check: read a small subset daily
CHECK_ARGS = ["--read-data-subset=10%"]  # bump to 10% weekly; 100% monthly if you dare

# Where to drop manifests
MANIFEST_DIR = Path.home() / "backup_manifests" / "restic"
MANIFEST_DIR.mkdir(parents=True, exist_ok=True)
# ------------------------------------

def cmd(*parts: str) -> List[str]:
    # Safe split for multi-part fragments; accepts already-split tokens
    out: List[str] = []
    for p in parts:
        if isinstance(p, list):
            out.extend(p)
        else:
            out.extend(shlex.split(p))
    return out

def run(cmdline: List[str], env=None, check=True) -> subprocess.CompletedProcess:
    return subprocess.run(cmdline, env=env, text=True, capture_output=True, check=check)

def get_password(ps_file: Path) -> str:
    if not ps_file.exists():
        raise FileNotFoundError(f"Password file not found: {ps_file}")
    with open(ps_file, 'r') as f:
        pw = f.read().strip()
    if not pw:
        raise RuntimeError("Restic password is empty.")
    return pw

def restic_env(restic_password: str) -> dict:
    env = os.environ.copy()
    env["RESTIC_PASSWORD"] = restic_password
    env["RESTIC_REPOSITORY"] = str(TARGET_PATH)
    return env

def ensure_repo_exists(env: dict) -> None:
    # `restic cat config` exits 0 if repo exists and is readable
    try:
        run(["restic", "cat", "config"], env=env, check=True)
    except subprocess.CalledProcessError:
        raise SystemExit(
            f"Could not read repo at {TARGET_PATH}. Did you run `restic init` with this password?"
        )

def restic_backup(env: dict, sources: List[str], exclude_file: Optional[Path]) -> dict:
    args = ["restic", "backup"]
    if exclude_file and exclude_file.exists():
        args += ["--exclude-file", str(exclude_file)]
    args += sources

    proc = run(args, env=env, check=True)
    # restic prints JSON only with --json; we’ll parse snapshot ID via `restic snapshots --json` afterward.
    return {"stdout": proc.stdout, "stderr": proc.stderr}

def latest_snapshots(env: dict) -> List[dict]:
    proc = run(["restic", "--json", "snapshots"], env=env, check=True)
    try:
        data = json.loads(proc.stdout)
        # restic --json snapshots returns a list of snapshot dicts
        return data if isinstance(data, list) else []
    except json.JSONDecodeError:
        return []

def restic_forget(env: dict) -> dict:
    args = ["restic", "forget", *FORGET_ARGS]
    proc = run(args, env=env, check=True)
    return {"stdout": proc.stdout, "stderr": proc.stderr}

def restic_check(env: dict) -> dict:
    args = ["restic", "check", *CHECK_ARGS]
    proc = run(args, env=env, check=True)
    return {"stdout": proc.stdout, "stderr": proc.stderr}

def write_manifest(snapshots_before: List[dict], snapshots_after: List[dict],
                   actions: dict) -> Path:
    ts = datetime.now(datetime.timezone.utc).isoformat()
    manifest = {
        "timestamp_utc": ts,
        "repo": str(TARGET_PATH),
        "sources": SOURCES,
        "forget_policy": FORGET_ARGS,
        "check_args": CHECK_ARGS,
        "counts": {
            "snapshots_before": len(snapshots_before),
            "snapshots_after": len(snapshots_after),
        },
        "latest_snapshot_ids": [s.get("short_id") or s.get("id") for s in sorted(
            snapshots_after, key=lambda x: x.get("time", ""), reverse=True)[:3]],
        "actions": actions,  # stdout/stderr tails for audit
        "host": os.uname().nodename if hasattr(os, "uname") else None,
    }
    outpath = MANIFEST_DIR / f"restic_{datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')}.json"
    with open(outpath, "w") as f:
        json.dump(manifest, f, indent=2)
    return outpath

def tail_text(text: str, max_chars: int = 2000) -> str:
    return text[-max_chars:] if len(text) > max_chars else text

def main(parent_id : str | None = None):



    try:
        password = get_password(ps_file=odin_config.restic_dropbox_job.password_file_path)
        env = restic_env(password)
        ensure_repo_exists(env)
        snaps_before = latest_snapshots(env)
        backup_res = restic_backup(env, SOURCES, EXCLUDE_FILE)
        forget_res = restic_forget(env)
        check_res = restic_check(env)
        snaps_after = latest_snapshots(env)

        actions = {
            "backup_stdout_tail": tail_text(backup_res["stdout"]),
            "backup_stderr_tail": tail_text(backup_res["stderr"]),
            "forget_stdout_tail": tail_text(forget_res["stdout"]),
            "forget_stderr_tail": tail_text(forget_res["stderr"]),
            "check_stdout_tail": tail_text(check_res["stdout"]),
            "check_stderr_tail": tail_text(check_res["stderr"]),
        }
        manifest_path = write_manifest(snaps_before, snaps_after, actions)
        print(f"[OK] Backup complete. Manifest → {manifest_path}")

    except subprocess.CalledProcessError as e:
        logger.exception("could not perform restic backup")
        logger.error(f"[ERR] Command failed: {' '.join(e.cmd)}\n")
        if e.stdout is not None:
            logger.error(e.stdout)
        if e.stderr is not None:
            logger.error(e.stderr)
        sys.exit(e.returncode)
    except Exception as e:
        logger.exception("could not perform restic backup")
        sys.exit(1)


if __name__ == "__main__":
    main()
