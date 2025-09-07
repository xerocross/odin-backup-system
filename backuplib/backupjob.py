from dataclasses import dataclass
from enum import Enum

class BackupJobResult(Enum):
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"

    def __str__(self):
        return self.value

