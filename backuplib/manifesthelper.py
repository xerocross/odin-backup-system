#!/usr/bin/env python3

from __future__ import annotations
from pathlib import Path
from datetime import datetime
from backuplib.checksumtools import compute_sha256
from backuplib.logging import setup_logging, Logger
from backuplib.filesutil import is_excluded
from pydeclarativelib.pydeclarativelib import IterConsumable, IterConsumable
from typing import List, TypedDict, Iterator, Any
import json
import os

class ManifestFileCreationFailureException(Exception):
    """Could not create the manifest files."""

class CouldNotGetFileInfoException(Exception):
    """Encountered an exception while trying to get file info."""

class CouldNotWriteFileInfoException(Exception):
    """Encountered an exception while trying to write file info."""

def write_manifest(root_dir : str, 
                   manifest_path_str : str,
                   manifest_info_path_str : str,
                   exclude_patterns : List[str] | None = None):

    root_path = Path(root_dir).resolve()
    logger : Logger = setup_logging(appName = "manifest-generator")

    class FileInfo(TypedDict):
        path: str
        checksum: str
        mod_time: str
        size_bytes: int

    def get_file_info(file_path : Path, rel_path : Path) -> FileInfo:
        try:
            stat: os.stat_result = file_path.stat()
            size_bytes : int = stat.st_size
            info : FileInfo = {
                "path": str(rel_path),
                "checksum": compute_sha256(file_path),
                "mod_time": datetime.fromtimestamp(stat.st_mtime).isoformat(),
                "size_bytes": size_bytes
            }
            return info
        except Exception as e:
            raise CouldNotGetFileInfoException() from e

    def preflight_check(manifest_path : Path, manifest_info_path: Path):
        if manifest_path.exists() or manifest_info_path.exists():
            return False
        return True


    def append_file_info_line(manifest_path : Path, info : FileInfo) -> None:
        print("attempting to append information")
        try:
            file_info_json_str = json.dumps(info, sort_keys=True)
            print(f"attempting to append {file_info_json_str} to file {manifest_path.as_posix()}")
            with open(manifest_path, 'a') as f:
                f.write(file_info_json_str + "\n")
                f.flush()
        except Exception as e:
            raise CouldNotWriteFileInfoException() from e
        

    def create_new_manifest_files(manifest_path : Path, manifest_info_path: Path) -> bool:
        try:
            if preflight_check(manifest_path=manifest_path, manifest_info_path=manifest_info_path):
                with open(manifest_path, 'w') as f:
                    f.write("")
                    f.flush()
                with open(manifest_info_path, 'w') as f:
                    f.write("")
                    f.flush()
                return True
        except Exception as e:
            raise ManifestFileCreationFailureException() from e


    def process_file(file_path: Path) -> None:
        print(f"processing {file_path.as_posix()}")
        if file_path.is_file():
            rel_path = file_path.relative_to(root_path)
            if is_excluded(rel_path, exclude_patterns):
                print("excluded")
                return
            
            try:
                file_info : FileInfo = get_file_info(file_path=file_path, rel_path=rel_path)
                print("file info")
                print(file_info)
                append_file_info_line(manifest_path=manifest_path, info=file_info)

            except CouldNotGetFileInfoException:
                logger.exception(f"could not get information for file {str(rel_path)}")
                # handle file err
            except CouldNotWriteFileInfoException:
                logger.exception(f"could not write information of file {str(rel_path)} to {manifest_path.as_posix()}")
            except Exception:
                logger.exception(f"could not process file {str(rel_path)}")

    def process_files(root_path: Path):
        fileGenerator : Iterator[Any] = root_path.rglob("*")
        iter_consume : IterConsumable = IterConsumable(fileGenerator)
        iter_consume.for_each(process_file)

    
    manifest_path = Path(manifest_path_str).resolve()
    manifest_info_path = Path(manifest_info_path_str).resolve()
    create_new_manifest_files(manifest_path=manifest_path, manifest_info_path=manifest_info_path)
    process_files(root_path=root_path)
    print(f"Manifest written to {args.output}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Generate a manifest of file checksums.")
    parser.add_argument("directory", help="Directory to scan")
    parser.add_argument("--output", default="manifest.txt", help="Path to output manifest file")
    parser.add_argument("--info", default="manifest.txt", help="Path to output manifest file")
    parser.add_argument("--exclude", nargs="*", default=[], help="Glob patterns to exclude (e.g., '*.tmp' or 'cache/*')")
    args = parser.parse_args()

    write_manifest(root_dir=args.directory, 
                   manifest_path_str=args.output, 
                   manifest_info_path_str=args.info, 
                   exclude_patterns=args.exclude)
    
