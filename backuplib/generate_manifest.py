#!/usr/bin/env python3

from pathlib import Path
from collections import defaultdict
import fnmatch
from datetime import datetime
import yaml
from backuplib.checksumtools import compute_sha256


def write_manifest(root_dir, 
                   manifest_path, 
                   format_type="yaml", 
                   exclude_patterns=None):
    print(f"Scanning contents of {root_dir}")
    root_path = Path(root_dir).resolve()

    with open(manifest_path, "w", encoding="utf-8") as manifest:

        if format_type == "tree":
            tree, _ = build_tree(root_path=root_path, 
                              exclude_patterns=exclude_patterns)
            for dir_path in sorted(tree):
                indent = "  " * len(dir_path.parts)
                manifest.write(f"{indent}{dir_path.as_posix()}/\n")
                for name, checksum in sorted(tree[dir_path]):
                    manifest.write(f"{indent}  {name}  {checksum}\n")
        elif format_type == "yaml":
            write_manifest_yaml(root_dir=root_dir, 
                                manifest_path=manifest_path, 
                                exclude_patterns=exclude_patterns)
        elif format_type == "flat":
            files = build_flat_list(root_path=root_path, exclude_patterns=exclude_patterns)
            for rel_path, checksum in sorted(files):
                manifest.write(f"{rel_path}, {checksum}\n")
        else:
            raise ValueError(f"Unknown format: {format_type}")
        
        
def build_tree(root_path):
    tree = defaultdict(list)
    errors = []
    for file_path in root_path.rglob("*"):
        if file_path.is_file():
            try:
                rel_path = file_path.relative_to(root_path)
                parent = rel_path.parent
                tree[parent].append((rel_path.name, compute_sha256(file_path)))
            except Exception as e:
                errors.append(file_path)
                print(f"Error processing {file_path}: {e}")
    return tree, errors

def is_excluded(path, exclude_patterns):
    path_string = path.as_posix()
    return any(fnmatch.fnmatch(path_string, pattern) for pattern in exclude_patterns)

def is_excluded_cleaned(path_str, exclude_patterns):
    print(f"checking whether to exclude \"{path_str}\"")
    print("exclude patterns:")
    print(exclude_patterns)
    if exclude_patterns is None:
        return False

    is_exclude = any(fnmatch.fnmatch(path_str, pattern) for pattern in exclude_patterns)
    print(f"excluded: {is_exclude}")
    return is_exclude

def build_flat_list(root_path, exclude_patterns):
    files = []
    for file_path in root_path.rglob("*"):
        if file_path.is_file():
            rel_path = file_path.relative_to(root_path)
            if is_excluded(rel_path, exclude_patterns):
                continue
            try:
                checksum = compute_sha256(file_path)
                files.append((rel_path.as_posix(), checksum))
            except Exception as e:
                print(f"Error processing {file_path}: {e}")
    return files

def build_file_list(root_path : Path, exclude_patterns=None):
    print(f"building file list from root: {root_path}")
    file_entries = []
    print("exclude pattterns:")
    print(exclude_patterns)
    for file_path in root_path.rglob("*"):
        if file_path.is_file():
            print(f"checking file {file_path}")
            rel_path = file_path.relative_to(root_path).as_posix()
            is_file_excluded = is_excluded_cleaned(rel_path, exclude_patterns)
            
            if is_file_excluded:
                print(f"excluding file {rel_path}")
                continue
            stat = file_path.stat()
            size_bytes = stat.st_size
            entry = {
                "path": rel_path,
                "checksum": compute_sha256(file_path),
                "mod_time": datetime.fromtimestamp(stat.st_mtime).isoformat(),
                "size_bytes": size_bytes
            }
            file_entries.append(entry)
    return file_entries


def write_manifest_yaml(root_dir, manifest_path, exclude_patterns=None):
    print(f"Scanning contents of {root_dir}")
    root_path = Path(root_dir).resolve()

    manifest_data = {
        "version": 1,
        "checksum_algorithm" : "sha256",
        "generated_at": datetime.now().isoformat(),
        "origin_root": str(root_path),
        "files": build_file_list(root_path, exclude_patterns)
    }

    with open(manifest_path, "w", encoding="utf-8") as f:
        yaml.dump(manifest_data, f, sort_keys=False, allow_unicode=True)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Generate a manifest of file checksums.")
    parser.add_argument("directory", help="Directory to scan")
    parser.add_argument("--output", default="manifest.txt", help="Path to output manifest file")
    parser.add_argument("--format", choices=["flat", "tree", "yaml"], default="flat",
                        help="Output format: 'flat' (CSV-like) or 'tree' (hierarchical)")
    parser.add_argument("--exclude", nargs="*", default=[], help="Glob patterns to exclude (e.g., '*.tmp' or 'cache/*')")
    args = parser.parse_args()

    write_manifest(args.directory, args.output, args.format)
    print(f"Manifest written to {args.output}")
