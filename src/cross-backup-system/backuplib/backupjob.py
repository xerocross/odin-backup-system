from dataclasses import dataclass
from enum import Enum

class BackupJobResult(Enum):
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"

    def __str__(self):
        return self.value


class JobState(Enum):
    STATE_EXPIRED = 1
    STATE_NOT_FOUND_SHOULD_UPDATE = 2
    STILL_CURRENT_SHOULD_SKIP = 3
    STATE_CHECK_FAILED_SHOULD_REBUILD = 4