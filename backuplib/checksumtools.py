#!/usr/bin/env python3

import hashlib, tempfile
from pathlib import Path
import os

def compute_sha256(path: str | Path, chunk_size: int = 1024 * 1024) -> str:
    """Return hex SHA-256 of file at `path`, streaming in chunks."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(chunk_size), b""):
            h.update(chunk)
    return h.hexdigest()

def write_sha256_sidecar(target: str | Path) -> Path:
    """
    Write a sidecar checksum file: `<target>.sha256`, contents:
    <HEX>  <BASENAME>\n
    Uses atomic replace to avoid half-written files.
    """
    target = Path(target)
    digest = compute_sha256(target)
    sidecar = target.with_suffix(target.suffix + ".sha256")  # keeps .tar.gz → .tar.gz.sha256

    # atomic write
    sidecar.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", delete=False, dir=sidecar.parent, encoding="utf-8") as tmp:
        tmp.write(f"{digest}  {target.name}\n")
        tmp_path = Path(tmp.name)
    os.replace(tmp_path, sidecar)  # atomic on same filesystem
    return sidecar
