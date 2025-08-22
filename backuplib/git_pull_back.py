#!/usr/bin/env python3
"""
git_pull.py — a small, dependency-free helper to run `git pull` safely from Python.

Usage examples:
  ./git_pull.py --repo /path/to/repo
  ./git_pull.py --repo /path/to/repo --remote origin --branch main --ff-only
  ./git_pull.py --repo . --rebase --timeout 300

Exit codes:
  0 on success, non-zero on failure. On failure, stderr explains why.
"""
import argparse
import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Optional, Tuple

def check_git_available() -> None:
    if shutil.which("git") is None:
        sys.stderr.write("Error: `git` not found on PATH. Install Git or adjust PATH.\n")
        sys.exit(127)

def ensure_repo(path: Path) -> None:
    if not path.exists():
        sys.stderr.write(f"Error: repo path does not exist: {path}\n")
        sys.exit(2)
    try:
        # `git -C <path> rev-parse --is-inside-work-tree` returns 'true' for a work tree
        out = subprocess.run(
            ["git", "-C", str(path), "rev-parse", "--is-inside-work-tree"],
            capture_output=True, text=True, check=True
        )
        if out.stdout.strip() not in {"true", "gitdir"}:
            sys.stderr.write(f"Error: {path} is not a Git work tree.\n")
            sys.exit(2)
    except subprocess.CalledProcessError as e:
        sys.stderr.write(f"Error: {path} is not a Git repo (rev-parse failed): {e.stderr}\n")
        sys.exit(2)

def run(cmd, timeout: Optional[int] = None, cwd: Optional[Path] = None) -> Tuple[int, str, str]:
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout, cwd=cwd)
    return proc.returncode, proc.stdout, proc.stderr

def git_pull(
    repo_path: Path,
    remote: str = "origin",
    branch: Optional[str] = None,
    rebase: bool = False,
    ff_only: bool = False,
    timeout: Optional[int] = 300,
) -> Tuple[int, str, str]:
    """
    Perform `git pull` in the given repo.
    Returns (returncode, stdout, stderr).
    """
    check_git_available()
    ensure_repo(repo_path)

    # Optional: fetch first for clearer failure modes (network, auth).
    fetch_cmd = ["git", "-C", str(repo_path), "fetch", remote]
    rc, out, err = run(fetch_cmd, timeout=timeout)
    if rc != 0:
        return rc, out, f"Fetch failed:\n{err}"

    # Build pull command
    pull_cmd = ["git", "-C", str(repo_path), "pull", remote]
    if branch:
        pull_cmd.append(branch)
    if rebase:
        pull_cmd.append("--rebase")
    if ff_only:
        pull_cmd.append("--ff-only")

    rc, out, err = run(pull_cmd, timeout=timeout)
    return rc, out, err

def parse_args(argv=None):
    p = argparse.ArgumentParser(description="Run `git pull` safely from Python.")
    p.add_argument(
        "--repo",
        required=True,
        help="Path to the Git repository (use '.' for current directory).",
    )
    p.add_argument("--remote", default="origin", help="Remote name (default: origin).")
    p.add_argument("--branch", default=None, help="Branch to pull (default: the current branch's upstream).")
    p.add_argument("--rebase", action="store_true", help="Use --rebase instead of merge.")
    p.add_argument("--ff-only", dest="ff_only", action="store_true", help="Require fast-forward (no merge commits).")
    p.add_argument(
        "--timeout",
        type=int,
        default=300,
        help="Timeout in seconds for fetch/pull (default: 300).",
    )
    return p.parse_args(argv)

def main(argv=None) -> int:
    args = parse_args(argv)
    repo_path = Path(args.repo).resolve()
    rc, out, err = git_pull(
        repo_path=repo_path,
        remote=args.remote,
        branch=args.branch,
        rebase=args.rebase,
        ff_only=args.ff_only,
        timeout=args.timeout,
    )
    # Stream outputs
    if out:
        sys.stdout.write(out)
    if err:
        sys.stderr.write(err)
    return rc

if __name__ == "__main__":
    sys.exit(main())
