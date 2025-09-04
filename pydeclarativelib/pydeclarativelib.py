
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
def _build_safe_binary(at: Path):
    desired_out_location = at
    desired_out_location.parent.mkdir(parents=True, exist_ok=True)
    try:
        with tempfile.TemporaryDirectory(dir=at.parent) as tmpdir:
            name = at.name
            print(f"name: {name}")
            out_path = Path(os.path.join(tmpdir, name))
            out_path_parent = out_path.parent
            if not out_path_parent.exists():
                print(f"the out path {out_path_parent} does not exist")

            print(f"out_path : {out_path}")
            with open(out_path, mode = 'wb+') as f:
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
    with _build_safe_binary(at=tarball_path) as f:
        _create_tarball(
                        fileobj = f,
                        input_path = input_path, 
                        exclude_patterns = excluding,
                        gz = True
                        )
        
