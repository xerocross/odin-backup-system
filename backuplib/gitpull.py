#!/usr/bin/env python3
"""
git_pull.py — a small, dependency-free helper to run `git pull` safely from Python,
with machine-readable outcomes.

Usage examples:
  ./git_pull.py --repo /path/to/repo
  ./git_pull.py --repo . --ff-only --porcelain
  ./git_pull.py --repo . --rebase --json

Possible result codes (printed when --porcelain or --json is used):
  - up_to_date      : no change; local already matches target
  - fast_forward    : updated by fast-forward
  - merge           : updated via merge commit
  - rebase          : updated via rebase
  - updated         : updated (couldn’t classify more specifically)
  - no_upstream     : current branch has no upstream set and no --branch given
  - error           : command failed (non-zero rc), see stderr

Exit codes:
  0 on success, non-zero on failure. On failure, stderr explains why.
"""
import argparse
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Optional, Tuple
from backuplib.audit import Tracker, StepRef
from dataclasses import dataclass


@dataclass
class QuickGitRepoSig:
    repo_path: str
    head_hash: str

class GitException(Exception):
    """Something went wrong in the git pull process."""
    pass

class GitNotFound(GitException):
    """Error: `git` not found on PATH. Install Git or adjust PATH."""
    pass

class PathNotAGitRepo(GitException):
    """Error: The given path was not a git repository."""
    pass


def generate_qsig(repo_path) -> QuickGitRepoSig:
    head_hash = get_git_headhash(repo_path)
    qsig = QuickGitRepoSig(
                repo_path = str(repo_path),
                head_hash = head_hash
            )
    return qsig


def check_git_available() -> None:
    if shutil.which("git") is None:
        raise GitNotFound

def ensure_repo(path: Path) -> None:
    if not path.exists():
        raise PathNotAGitRepo
    try:
        out = subprocess.run(
            ["git", "-C", str(path), "rev-parse", "--is-inside-work-tree"],
            capture_output=True, text=True, check=True
        )
        if out.stdout.strip() not in {"true", "gitdir"}:
            sys.stderr.write(f"Error: {path} is not a Git work tree.\n")

            raise PathNotAGitRepo

    except subprocess.CalledProcessError as e:
        sys.stderr.write(f"Error: {path} is not a Git repo (rev-parse failed): {e.stderr}\n")
        raise PathNotAGitRepo

def run(cmd, timeout: Optional[int] = None, cwd: Optional[Path] = None) -> Tuple[int, str, str]:
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout, cwd=cwd)
    return proc.returncode, proc.stdout, proc.stderr

def git_rev_parse(repo: Path, ref: str) -> Tuple[int, str, str]:
    return run(["git", "-C", str(repo), "rev-parse", ref])

def is_ancestor(repo: Path, anc: str, desc: str) -> bool:
    rc, _, _ = run(["git", "-C", str(repo), "merge-base", "--is-ancestor", anc, desc])
    return rc == 0

def has_second_parent(repo: Path, ref: str) -> bool:
    rc, _, _ = run(["git", "-C", str(repo), "rev-parse", f"{ref}^2"])
    return rc == 0

def detect_target_ref(repo: Path, remote: str, branch: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    """
    Returns (target_ref, error_message). target_ref is a rev spec to compare/pull from.
    """
    if branch:
        return f"{remote}/{branch}", None
    # Try upstream of current branch
    rc, upstream_ref, err = run(["git", "-C", str(repo), "rev-parse", "--abbrev-ref", "--symbolic-full-name", "@{u}"])
    if rc != 0:
        return None, "no_upstream"
    return upstream_ref.strip(), None

def get_git_headhash(
        repo_path: Path
    ):
    rc, stdout, stderr = run(["git", "-C", repo_path, "rev-parse", "HEAD"])
    if (rc != 0):
        raise GitException
    return stdout.strip()

def git_pull(
    repo_path: Path,
    remote: str = "origin",
    branch: Optional[str] = None,
    rebase: bool = False,
    ff_only: bool = False,
    timeout: Optional[int] = 300,
    want_machine: bool = False,
    want_json: bool = False,
) -> Tuple[int, str, str, dict]:
    """
    Perform `git pull` in the given repo.
    Returns (returncode, stdout, stderr, summary_dict).
    """
    summary = {"result": None, "before": None, "after": None, "target": None, "remote": remote, "branch": branch}
    check_git_available()
    ensure_repo(repo_path)

    # Identify target ref for comparison
    target_ref, errcode = detect_target_ref(repo_path, remote, branch)
    if not target_ref:
        summary["result"] = "no_upstream"
        return 1, "", "No upstream set for current branch and no --branch provided.\n", summary
    summary["target"] = target_ref

    # Record current HEAD before
    rc, before_sha, err = git_rev_parse(repo_path, "HEAD")
    if rc != 0:
        return rc, "", f"Failed to resolve HEAD: {err}", summary
    before_sha = before_sha.strip()
    summary["before"] = before_sha

    # Fetch first
    fetch_cmd = ["git", "-C", str(repo_path), "fetch", remote]
    frc, fout, ferr = run(fetch_cmd, timeout=timeout)
    if frc != 0:
        summary["result"] = "error"
        return frc, fout, f"Fetch failed:\n{ferr}", summary

    # Get remote/target sha after fetch
    rc, target_sha, err = git_rev_parse(repo_path, target_ref)
    if rc != 0:
        summary["result"] = "error"
        return rc, "", f"Failed to resolve target ref '{target_ref}': {err}", summary
    target_sha = target_sha.strip()

    # Quick classification before pulling
    if before_sha == target_sha:
        summary["result"] = "up_to_date"
        # Nothing to do; mirror git's behavior and return success without running pull
        return 0, "Already up to date.\n", "", summary

    # If fast-forward possible, we can classify ahead of time
    ff_possible = is_ancestor(repo_path, before_sha, target_sha)

    # Build pull cmd
    pull_cmd = ["git", "-C", str(repo_path), "pull", remote]
    if branch:
        pull_cmd.append(branch)
    if rebase:
        pull_cmd.append("--rebase")
    if ff_only:
        pull_cmd.append("--ff-only")

    prc, pout, perr = run(pull_cmd, timeout=timeout)
    if prc != 0:
        summary["result"] = "error"
        return prc, pout, perr, summary

    # After state
    rc, after_sha, err = git_rev_parse(repo_path, "HEAD")
    if rc != 0:
        summary["result"] = "error"
        return rc, "", f"Failed to resolve new HEAD: {err}", summary
    after_sha = after_sha.strip()
    summary["after"] = after_sha

    # Classify outcome
    if after_sha == before_sha:
        # Extremely rare (e.g., hooks), treat as up-to-date
        summary["result"] = "up_to_date"
    elif ff_possible and after_sha == target_sha:
        summary["result"] = "fast_forward"
    else:
        # Distinguish merge vs rebase if possible
        if has_second_parent(repo_path, "HEAD"):
            summary["result"] = "merge"
        elif rebase:
            summary["result"] = "rebase"
        else:
            # Could be rebase via config, or other update
            summary["result"] = "updated"

    return 0, pout, perr, summary

def parse_args(argv=None):
    p = argparse.ArgumentParser(description="Run `git pull` safely from Python, with machine-readable outcomes.")
    p.add_argument("--repo", required=True, help="Path to the Git repository (use '.' for current directory).")
    p.add_argument("--remote", default="origin", help="Remote name (default: origin).")
    p.add_argument("--branch", default=None, help="Branch to pull (default: the current branch's upstream).")
    p.add_argument("--rebase", action="store_true", help="Use --rebase instead of merge.")
    p.add_argument("--ff-only", dest="ff_only", action="store_true", help="Require fast-forward (no merge commits).")
    p.add_argument("--timeout", type=int, default=300, help="Timeout in seconds for fetch/pull (default: 300).")
    p.add_argument("--porcelain", action="store_true", help="Print a single machine-readable result code.")
    p.add_argument("--json", dest="as_json", action="store_true", help="Print a JSON summary.")
    return p.parse_args(argv)

def main(argv=None) -> int:
    args = parse_args(argv)

    repo_path = Path(args.repo).resolve()
    rc, out, err, summary = git_pull(
        repo_path=repo_path,
        remote=args.remote,
        branch=args.branch,
        rebase=args.rebase,
        ff_only=args.ff_only,
        timeout=args.timeout,
        want_machine=args.porcelain or args.as_json,
        want_json=args.as_json,
    )
    # Stream outputs as usual
    if out:
        sys.stdout.write(out)
    if err:
        sys.stderr.write(err)

    # Machine-readable outputs last
    if args.as_json:
        sys.stdout.write(json.dumps(summary) + "\n")
    elif args.porcelain and summary.get("result"):
        sys.stdout.write(f"{summary['result']}\n")

    return rc

if __name__ == "__main__":
    sys.exit(main())
