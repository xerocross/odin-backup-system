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

class GPGError(RuntimeError): pass


def gpg_sign_detached(
    artifact: Path,
    *,
    signer: str | None = None,      # key id / fingerprint / uid to sign with
    armor: bool = False,            # True -> .asc, False -> .sig (binary)
    homedir: Path | None = None,    # use a dedicated keyring dir if you want
    digest_algo: str = "SHA256",
    output: Path | None = None,     # override signature path
) -> Path:
    """
    Create a detached signature for `artifact` using gpg.
    Returns the path to the signature file.
    """
    artifact = Path(artifact)
    if not artifact.is_file():
        raise FileNotFoundError(f"Artifact not found: {artifact}")

    sig_ext = ".asc" if armor else ".sig"
    if output is None:
        output = artifact.with_suffix(artifact.suffix + sig_ext)

    # Build the command.
    cmd = ["gpg", "--batch", "--yes","--pinentry-mode","loopback", "--passphrase-fd", "0", "--detach-sign", f"--digest-algo={digest_algo}"]
    if armor:
        cmd.append("--armor")
    if signer:
        cmd += ["--local-user", signer]
    if homedir:
        cmd += ["--homedir", str(homedir)]

    # If you need non-interactive passphrase use loopback.
    env = os.environ.copy()

    # We write to a temp file and then atomically rename.
    output = Path(output)
    tmpdir = output.parent
    tmpdir.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(prefix=output.name, dir=tmpdir, delete=False) as tf:
        tmp_path = Path(tf.name)

    cmd += ["--output", str(tmp_path), str(artifact)]

    # Run gpg and capture output for logs.
    with open("/home/adam/.backup-secrets/gpg_sign.pass","rb") as f:
        proc = subprocess.run(cmd, stdin=f, capture_output=True, text=True)
    if proc.returncode != 0:
        # Clean up temp file on failure
        try: tmp_path.unlink(missing_ok=True)
        except Exception: pass
        raise GPGError(f"gpg sign failed ({proc.returncode}). stderr:\n{proc.stderr.strip()}")

    # fsync then atomic rename for durability
    with open(tmp_path, "rb") as f:
        os.fsync(f.fileno())
    os.replace(tmp_path, output)
    return output


def gpg_verify_detached(artifact: Path, signature: Path, *, homedir: Path | None = None) -> bool:
    """Return True if signature verifies, False otherwise (no exception)."""
    cmd = ["gpg", "--batch", "--verify"]
    if homedir:
        cmd += ["--homedir", str(homedir)]
    cmd += [str(signature), str(artifact)]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    return proc.returncode == 0
