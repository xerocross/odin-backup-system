from dataclasses import dataclass, List

@dataclass
class QuickManifestSig:
    root: str
    exclude: List[str]
    file_count: int
    latest_mtime_ns : int
    total_bytes: int