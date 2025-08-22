from pathlib import Path
import tempfile
import subprocess
import os

class GpgError(RuntimeError):
    pass

def encrypt_with_gpg_atomic(
    plaintext_path: str | Path,
    recipient: str,
    output_path: str | Path | None = None,
    *,
    armor: bool = False,
    extra_args: list[str] | None = None,
) -> Path:
    """
    Encrypt `plaintext_path` to `output_path` atomically:
      - write to a temp file in the destination dir
      - replace() into final name (atomic on same filesystem)

    If `output_path` is None, derives:
      foo.tar.gz → foo.tar.gz.gpg (or .asc if armor=True)

    Raises GpgError on failure, FileExistsError if target exists.
    """
    pt = Path(plaintext_path)
    if output_path is None:
        suffix = ".asc" if armor else ".gpg"
        output_path = Path(str(pt) + suffix)
    else:
        output_path = Path(output_path)

    # don’t clobber an existing artifact
    if output_path.exists():
        raise FileExistsError(output_path)

    output_dir = output_path.parent
    output_dir.mkdir(parents=True, exist_ok=True)

    # temp file lives in the same dir to keep replace() atomic
    with tempfile.NamedTemporaryFile(
        prefix=output_path.name + ".",
        suffix=".part",
        dir=output_dir,
        delete=False,
    ) as tmp:
        tmp_path = Path(tmp.name)

    cmd = ["gpg", "--batch", "--yes", "--encrypt", "--recipient", recipient, "--output", str(tmp_path)]
    if armor:
        cmd.append("--armor")
    if extra_args:
        cmd.extend(extra_args)
    cmd.append(str(pt))

    try:
        # capture stderr for logs, and fail fast on non-zero
        res = subprocess.run(cmd, text=True, capture_output=True, check=False)
        if res.returncode != 0:
            # clean up the temp file if gpg failed
            try: tmp_path.unlink(missing_ok=True)
            finally:
                raise GpgError(
                    f"GPG failed (code {res.returncode}).\n"
                    f"stdout:\n{res.stdout}\n"
                    f"stderr:\n{res.stderr}"
                )
        # success → atomic publish
        os.replace(tmp_path, output_path)
        return output_path
    except Exception as e:
        # ensure temp is gone on *any* exception path
        try: tmp_path.unlink(missing_ok=True)
        finally: pass
        raise e
