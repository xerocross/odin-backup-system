import fnmatch
from pathlib import Path
from typing import Iterable, Callable
import json, hashlib
import os, tempfile
from dataclasses import dataclass
from typing import Dict, List

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

def quick_scan_signature(root: Path, exclude: Iterable[str]) -> QuickManifestSig:
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


    qsig = QuickManifestSig(
                            root = str(root.resolve()),
                            exclude = list(exclude),
                            latest_mtime_ns = latest_mtime_ns,
                            file_count= file_count,
                            total_bytes=total_bytes
                           )
    return qsig


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