#!/usr/bin/env python3

from __future__ import annotations
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Optional, Tuple, List, Dict, Any
from auditlib.audit import Tracker
from dataclasses import dataclass
from backuplib.logging import setup_logging, WithContext

global_log = setup_logging(level="INFO", appName="git_pull_module")

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

class FetchFailed(GitException):
    """Error: The given path was not a git repository."""
    pass

class GitPullFailure(GitException):
    """Error: An error occurred during git pull"""
    pass

def generate_qsig(repo_path : Path) -> QuickGitRepoSig:
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
            _ = sys.stderr.write(f"Error: {path} is not a Git work tree.\n")
            raise PathNotAGitRepo

    except subprocess.CalledProcessError as e:
        _ = sys.stderr.write(f"Error: {path} is not a Git repo (rev-parse failed): {e.stderr}\n")
        raise PathNotAGitRepo

def run(cmd : List[str], timeout: Optional[int] = None, cwd: Optional[Path] = None) -> Tuple[int, str, str]:
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
    rc, upstream_ref, _ = run(["git", "-C", str(repo), "rev-parse", "--abbrev-ref", "--symbolic-full-name", "@{u}"])
    if rc != 0:
        return None, "no_upstream"
    return upstream_ref.strip(), None

def get_git_headhash(
        repo_path: Path
    ):
    rc, stdout, _ = run(["git", "-C", str(repo_path), "rev-parse", "HEAD"])
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
    *,
    tracker: Tracker,
    run_id: str
) -> Tuple[int, str, str, Dict[str, Any]]:
    """
    Perform `git pull` in the given repo.
    Returns (returncode, stdout, stderr, summary_dict).
    """
    log = WithContext(global_log, {"run_log_id": run_id})
    log.debug("starting git pull operation")
    summary = {"result": None, "before": None, "after": None, "target": None, "remote": remote, "branch": branch}
    
    with tracker.record_step(run_id =run_id, 
                             name = "ensure git repo"
                             ) as rec:
        check_git_available()
        ensure_repo(repo_path)
        rec["status"] = "success"


    # Identify target ref for comparison
    target_ref, errcode = detect_target_ref(repo_path, remote, branch)
    if not target_ref:
        message = "No upstream set for current branch and no --branch provided.\n"
        log.error(message)
        summary["result"] = "no_upstream"
        return 1, "", message, summary
    summary["target"] = target_ref

    with tracker.record_step(run_id, "record head") as rec:
        # Record current HEAD before
        rc, before_sha, err = git_rev_parse(repo_path, "HEAD")
        if rc != 0:
            rec["status"] = "failed"
            raise GitException
        before_sha = before_sha.strip()
        summary["before"] = before_sha
        rec["status"] = "success"

    with tracker.record_step(run_id, "perform fetch") as rec:
        # Fetch first
        fetch_cmd = ["git", "-C", str(repo_path), "fetch", remote]
        frc, fout, ferr = run(fetch_cmd, timeout=timeout)
        if frc != 0:
            rec["status"] = "failed"
            raise FetchFailed

    with tracker.record_step(run_id, "get target sha") as rec:
        # Get remote/target sha after fetch
        rc, target_sha, err = git_rev_parse(repo_path, target_ref)
        if rc != 0:
            rec["status"] = "failed"
            rec["message"] = f"Failed to resolve target ref '{target_ref}': {err}"
            summary["result"] = "error"
            raise GitException
        rec["status"] = "success"
        target_sha = target_sha.strip()

    with tracker.record_step(run_id, "classify update") as rec:
        # Quick classification before pulling
        if before_sha == target_sha:
            rec["status"] = "success"
            summary["result"] = "up_to_date"
            rec["message"] = "up_to_date"
            # Nothing to do; mirror git's behavior and return success without running pull
            log.info("already up to date")
            return 0, "Already up to date.\n", "", summary

        # If fast-forward possible, we can classify ahead of time
        ff_possible = is_ancestor(repo_path, before_sha, target_sha)
        rec["message"] = "ff possible"
        rec["status"] = "success"

    with tracker.record_step(run_id, "perform git pull operation") as rec:
        # Build pull cmd
        pull_cmd = ["git", "-C", str(repo_path), "pull", remote]
        if branch:
            rec["message"] = "branch"
            pull_cmd.append(branch)
        if rebase:
            rec["message"] = rec["message"] + ": rebase"
            pull_cmd.append("--rebase")
        if ff_only:
            rec["message"] = rec["message"] + ": ff only"
            pull_cmd.append("--ff-only")

        prc, pout, perr = run(pull_cmd, timeout=timeout)
        if prc != 0:
            rec["status"] = "failed"
            summary["result"] = "error"
            return prc, pout, perr, summary
        else:
            rec["status"] = "success"

    with tracker.record_step(run_id, "capture after state") as rec:
        # After state
        rc, after_sha, err = git_rev_parse(repo_path, "HEAD")
        if rc != 0:
            message = f"Failed to resolve new HEAD: {err}"
            log.error(message)
            rec["status"] = "failed"
            summary["result"] = "error"
            rec["message"] = message
            return rc, "", message, summary
        after_sha = after_sha.strip()
        summary["after"] = after_sha

        # Classify outcome
        if after_sha == before_sha:
            # Extremely rare (e.g., hooks), treat as up-to-date
            msg = "up_to_date"
            log.info(msg)
            summary["result"] = msg
            rec["message"] = msg
        elif ff_possible and after_sha == target_sha:
            msg = "fast_forward"
            log.info(msg)
            summary["result"] = msg
            rec["message"] = msg
        else:
            # Distinguish merge vs rebase if possible
            if has_second_parent(repo_path, "HEAD"):
                summary["result"] = "merge"
                rec["message"] = "merge"
            elif rebase:
                summary["result"] = "rebase"
                rec["message"] = "rebase"
            else:
                # Could be rebase via config, or other update
                rec["message"] = "updated"
                summary["result"] = "updated"

    return 0, pout, perr, summary

if __name__ == "__main__":
    sys.exit()
