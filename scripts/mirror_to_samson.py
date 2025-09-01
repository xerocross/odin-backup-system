#!/usr/bin/env python3
import argparse, subprocess, sys
from pathlib import Path
from backuplib.logging import setup_logging
from backuplib.configloader import OdinConfig, load_config

odin_cfg: OdinConfig = load_config()
logger = setup_logging()

exclude_list = [
    ".git/",
    "**/__pycache__/",
    "node_modules/",
    ".venv/",
    ".cache/",
    "*.tmp",
    "*.swp",
    ".DS_Store",
]





def run(cmd: list[str]) -> int:
    logger.info("starting rsync backup of odin")
    return subprocess.call(cmd)

def main():
    p = argparse.ArgumentParser(description="Mirror Odin to a destination (rsync).")
    p.add_argument("--src", required=True, help="Path to Odin source directory")
    p.add_argument("--dst", required=True, help="Destination root (e.g., /mnt/samson/backups)")
    p.add_argument("--exclude-file", help="Path to rsync exclude file (one pattern per line)")
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args()

    src = str(Path(args.src).expanduser().resolve())
    dst_root = Path(args.dst).expanduser().resolve()
    dst_root.mkdir(parents=True, exist_ok=True)
    dst = str((dst_root / Path(src).name))

    cmd = ["rsync","-aHAX","--delete","--human-readable","--info=STATS2,PROGRESS2"]
    if args.dry_run:
        cmd.append("--dry-run")
    if args.exclude_file:
        cmd += ["--exclude-from", str(Path(args.exclude_file).expanduser().resolve())]
    cmd += [src + "/", dst + "/"]

    print(">>", " ".join(cmd))
    code = run(cmd)
    if code == 0:
        print("[ok] mirror complete:", src, "->", dst)
    else:
        print("[error] rsync exit code:", code, file=sys.stderr)
    sys.exit(code)

if __name__ == "__main__":
    main()
