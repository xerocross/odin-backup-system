#!/usr/bin/env python3

import hashlib, tempfile
from pathlib import Path
import os, json

class InvalidJSONException(Exception):
    '''Exception: attempted to canonicalize invalid json'''

# Helpers
def canonicalize_json(text):
    if text is None:
        return None
    try:
        obj = json.loads(text)
    except Exception:
        raise InvalidJSONException
        # Not valid JSON (or you stored raw text); hash the raw bytes
    # Canonical dump for stable hashing (order/whitespace independent)
    return json.dumps(obj, separators=(",", ":"), sort_keys=True, ensure_ascii=False)

def sha256_hex(s):
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


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
