
from backuplib.configloader import OdinConfig, load_config
from backuplib.checksumtools import sha256_string
from pathlib import Path
import json
from typing import Dict, List, Optional
from dataclasses import dataclass, asdict

odinConfig: OdinConfig = load_config()

stored_manifest_state_json = None


def load_manifest_state():
    if stored_manifest_state_json is not None:
        return stored_manifest_state_json
    manifest_dir = odinConfig.manifest_dir
    manifest_state_name = odinConfig.manifest_state_name
    manifest_state_path = manifest_dir / manifest_state_name
    with open(manifest_state_path, 'r', encoding="utf-8") as f:
        manifest_state_json = json.load(f)
        stored_manifest_state_json = manifest_state_json
    return manifest_state_json
    
def manifest_state_input_sig():
    manifest_state_json = load_manifest_state()
    return manifest_state_json["init_sig_hex"]

def manifest_state_output_sig():
    manifest_state_json = load_manifest_state()
    return manifest_state_json["output_sig_hex"]


def _sorted(x):
    if (isinstance(x, list)):
        sorted_list = list.sorted()
        return sorted_list
    return x

def hash_config(config: dict) -> str:
    config_keys = config.keys().sorted()

    canon_config = {}
    for k in config_keys:
        val = config(k)
        canon_config[k] = _sorted(val)
        canon_config[k] = config[k]
        config_json = json.dump(canon_config)
    return sha256_string(config_json)


def get_hash(statefile_path : Path):
    with open(statefile_path, 'r', encoding="utf-8") as f:
        state_json = json.load(f)
    return state_json.get("hash")

def get_upstream_hash(statefile_path : Path):
    with open(statefile_path, 'r', encoding="utf-8") as f:
        state_json = json.load(f)
    return state_json.get("upstream_hash")

#upstream_hash



@dataclass
class JobRunSignatures:
    init_sig: str
    output_sig: str
    timestamp: str


# @dataclass
# class ManifestSigInfo(dataclass):
#     pass


# def read_manifest_hash(upstream_state_path: Path) -> ManifestSigInfo:
#     """
#     Reads the upstream manifest job's state JSON and returns:
#       {"manifest_hash": "...", "manifest_state_ts": "..."}.
#     By default expects {"output_hash": "..."} but keep it configurable.
#     """
#     manifest_state = json.loads(upstream_state_path.read_text(encoding="utf-8"))
#     manifest_out_hash = manifest_state["output_sig_hex"]
#     manifest_init_hash = manifest_state["init_sig_hex"]
#     manifest_timestamp = manifest_state["timestamp"]

#     manifestSigInfo = ManifestSigInfo(
#         init_sig= manifest_init_hash,
#         output_sig=manifest_out_hash,
#         timestamp=manifest_timestamp
#     )
#     return manifestSigInfo


def generate_state_file():
    pass


