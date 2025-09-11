
from pathlib import Path
import tempfile
import os
import tarfile
from typing import List
from fnmatch import fnmatch
from contextlib import contextmanager


class InputTypeException(Exception):
    """Input type was not acceptable"""
    pass

class SafeBuildNotCompleted(Exception):
    """A safe-build procedure failed"""
    pass


@contextmanager
def _build_safe(at: Path, in_binary = False):
    mode = 'wb+' if in_binary else 'w+'

    desired_out_location = at
    desired_out_location.parent.mkdir(parents=True, exist_ok=True)
    try:
        with tempfile.TemporaryDirectory(dir=at.parent) as tmpdir:
            name = at.name
            out_path = Path(os.path.join(tmpdir, name))
            out_path_parent = out_path.parent
            if not out_path_parent.exists():
                print(f"the out path {out_path_parent} does not exist")

            with open(out_path, mode = mode) as f:
                yield f
                # ensure all bytes are on disk before replace
                f.flush()
                os.fsync(f.fileno())
            os.replace(out_path, desired_out_location)
    except Exception as e:
        raise SafeBuildNotCompleted() from e


def _path_matches_any(rel: str, patterns: list[str]) -> bool:
    """Match POSIX-style relative path against glob patterns (supports **)."""
    return any(fnmatch(rel, pat) for pat in patterns)

def _create_tarball(
                    fileobj,
                    input_path: Path, 
                    exclude_patterns: List[str],
                    gz: bool = True
                ) -> None:
    """
    Create a tar (optionally gzip) stream into fileobj containing input_path contents (no top-level nesting).
    Excludes are matched against POSIX-style relative paths.
    """
    mode = 'w:gz' if gz else 'w'
    with tarfile.open(name=None, mode = mode, fileobj=fileobj) as tar:
        for p in input_path.rglob("*"):
            if not p.is_file():
                continue  # avoid redundant recursion
            rel = p.relative_to(input_path).as_posix()
            if exclude_patterns and _path_matches_any(rel, exclude_patterns):
                continue
            tar.add(p, arcname=rel)


def make_a_tarball(of_dir: Path, at : Path, excluding : List[str]):
    if not isinstance(of_dir, Path):
        raise InputTypeException("of_dir must be a Path")
    
    tarball_path = at
    input_path = of_dir
    with _build_safe(at=tarball_path, in_binary=True) as f:
        _create_tarball(
                        fileobj = f,
                        input_path = input_path, 
                        exclude_patterns = excluding,
                        gz = True
                        )
        

@contextmanager
def safe_open_for_writing(to_path: Path):
    path = to_path
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = None
    try:
        with tempfile.NamedTemporaryFile(
            "w", delete=False, dir=path.parent,
            prefix=path.name + ".", suffix=".part", encoding="utf-8"
        ) as tmp:
            yield tmp
            tmp.flush()
            os.fsync(tmp.fileno())  # durability before publish
            tmp_path = Path(tmp.name)
        os.replace(tmp_path, path)  # atomic publish => tmp name disappears
    except Exception:
        if tmp_path is not None:
            try: os.unlink(tmp_path)   # cleanup only on failure
            except OSError: pass
        raise

def write_text_atomic(at: Path, the_text: str):
    path = at
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = None
    try:
        with tempfile.NamedTemporaryFile(
            "w", delete=False, dir=path.parent,
            prefix=path.name + ".", suffix=".part", encoding="utf-8"
        ) as tmp:
            tmp.write(the_text)
            tmp.flush()
            os.fsync(tmp.fileno())  # durability before publish
            tmp_path = Path(tmp.name)
        os.replace(tmp_path, path)  # atomic publish => tmp name disappears
    except Exception:
        if tmp_path is not None:
            try: os.unlink(tmp_path)   # cleanup only on failure
            except OSError: pass
        raise

def is_path_matches_amy_pattern(path: str, patterns: list[str]) -> bool:
    """Match POSIX-style relative path against glob patterns (supports **)."""
    return _path_matches_any(rel=path, patterns=patterns)

