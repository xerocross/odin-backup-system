import fnmatch
from pathlib import Path
from typing import Iterable, Callable
import json, hashlib
import os, tempfile
from dataclasses import dataclass
from typing import Dict, List, Tuple
from backuplib.checksumtools import sha256_string


@dataclass
class QuickManifestSig:
    root: str
    exclude: List[str]
    file_count: int
    latest_mtime_ns : int
    total_bytes: int




def is_excluded(path: Path, exclude_patterns: Iterable[str]):
    path_string = path.as_posix()
    return any(fnmatch.fnmatch(path_string, pattern) for pattern in exclude_patterns)

def quick_scan_signature(root: Path, exclude: List) -> QuickManifestSig:
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
        dirnames[:] = [d for d in dirnames if not is_excluded(rel_root / d, exclude)]
        for fn in filenames:
            rel = rel_root / fn
            if is_excluded(rel, exclude):
                continue
            p = Path(dirpath) / fn
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


@dataclass
class QuickManifestScan:
    root: str
    exclude: List[str]
    file_count: int
    latest_mtime_ns : int
    total_bytes: int


def hash_quick_manifest_scan(quick_scan : QuickManifestScan):
    digest = f"{sha256_string(str(quick_scan.file_count))}\
        {sha256_string(str(quick_scan.latest_mtime_ns))}\
            {sha256_string(str(quick_scan.total_bytes))}"
    return sha256_string(digest)

def file_to_lines_list(from_file: Path):
    with open(from_file, 'r') as f:
        lines = f.readlines()
    return lines


def digest(obj) -> str:
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