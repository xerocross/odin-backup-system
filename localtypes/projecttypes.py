
from __future__ import annotations
from typing import TypedDict, Any, Dict


class JobStageInfo(TypedDict):
    success: bool
    data : Dict[str, Any] | None

class AuditStageRecord(TypedDict):
    success : str
    message : str | None