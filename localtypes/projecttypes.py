
from __future__ import annotations
from typing import TypedDict, Any, Dict


class JobStageInfo(TypedDict):
    success: bool
    data : Dict[str, Any] | None

class AuditStageRecord(TypedDict):
    status : str
    message : str | None