from __future__ import annotations
import fnmatch
from pathlib import Path
from typing import Iterable
import json, hashlib
import os, tempfile
from dataclasses import dataclass
from typing import List, Any
from backuplib.checksumtools import sha256_string
import errno


@dataclass
class QuickManifestSig:
    root: str
    exclude: List[str]
    file_count: int
    latest_mtime_ns : int
    total_bytes: int


@dataclass
class QuickManifestScan:
    root: str
    exclude: List[str]
    file_count: int
    latest_mtime_ns : int
    total_bytes: int

def is_excluded(path: Path, exclude_patterns: Iterable[str] | None) -> bool:
    path_string = path.as_posix()
    if not exclude_patterns:
        return False
    else:
        return any(fnmatch.fnmatch(path_string, pattern) for pattern in exclude_patterns)

def quick_scan_signature(root: Path, exclude: List[str]) -> QuickManifestSig:
    """
    Cheap signal for 'did anything relevant change?':
      - latest mtime (ns) of any included file
      - total file count
      - total bytes
    """
    latest_mtime_ns = 0
    total_bytes = 0
    file_count = 0
    for dirpath, dirnames, filenames in os.walk(root):
        rel_root = Path(dirpath).relative_to(root)
        # prune excluded dirs (in-place)
        new_dirnames = [d for d in dirnames if not is_excluded(rel_root / d, exclude)]
        dirnames.clear()
        dirnames.extend(new_dirnames)
        for fn in filenames:
            rel = rel_root / fn
            if is_excluded(rel, exclude):
                continue
            p: Path  = Path(dirpath) / fn
            if not p.exists():
                continue
            st = p.stat()
            file_count += 1
            total_bytes += st.st_size
            if st.st_mtime_ns > latest_mtime_ns:
                latest_mtime_ns = st.st_mtime_ns
    return QuickManifestSig(
        root = str(root),
        exclude = exclude,
        file_count = file_count,
        latest_mtime_ns = latest_mtime_ns,
        total_bytes = total_bytes
    )


def hash_quick_manifest_scan(quick_scan : QuickManifestSig):
    digest = f"{sha256_string(str(quick_scan.file_count))}\
        {sha256_string(str(quick_scan.latest_mtime_ns))}\
            {sha256_string(str(quick_scan.total_bytes))}"
    return sha256_string(digest)

def file_to_lines_list(from_file: Path):
    with open(from_file, 'r') as f:
        lines = f.readlines()
    return lines


def digest(obj: Any) -> str:
    data = json.dumps(obj, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(data).hexdigest()

def atomic_write_text(path: Path, text: str):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = None
    try:
        with tempfile.NamedTemporaryFile(
            "w", delete=False, dir=path.parent,
            prefix=path.name + ".", suffix=".part", encoding="utf-8"
        ) as tmp:
            tmp.write(text)
            tmp.flush()
            os.fsync(tmp.fileno())  # durability before publish
            tmp_path = Path(tmp.name)
        os.replace(tmp_path, path)  # atomic publish => tmp name disappears
    except Exception:
        if tmp_path is not None:
            try: os.unlink(tmp_path)   # cleanup only on failure
            except OSError: pass
        raise


def preflight_check_write(target_path: Path) -> None:
    """
    Perform a preflight check to ensure the given path is writable
    and that no existing file or directory will be overwritten.

    Raises:
        FileExistsError: If the target already exists.
        PermissionError: If the directory is not writable.
        FileNotFoundError: If the parent directory does not exist.
        OSError: For other OS-level issues.
    """
    target_path = Path(target_path)

    # Check parent directory
    parent = target_path.parent
    if not parent.exists():
        raise FileNotFoundError(errno.ENOENT, f"Parent directory does not exist: {parent}")
    if not parent.is_dir():
        raise NotADirectoryError(errno.ENOTDIR, f"Parent path is not a directory: {parent}")

    # Check for existing file or dir
    if target_path.exists():
        raise FileExistsError(errno.EEXIST, f"Target already exists: {target_path}")

    # Check writability by attempting to open a test file descriptor
    try:
        with open(parent / ".__write_test__", "w") as f:
            pass
        os.remove(parent / ".__write_test__")
    except PermissionError:
        raise PermissionError(errno.EACCES, f"Directory is not writable: {parent}")
    except OSError as e:
        raise OSError(e.errno, f"Unexpected OS error while checking writability: {e.strerror}")
