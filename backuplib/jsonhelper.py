from dataclasses import dataclass, asdict, is_dataclass
from pathlib import Path
from datetime import date, datetime
from enum import Enum
import json

class EnhancedJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        # dataclasses → dict (recursively)
        if is_dataclass(obj):
            return asdict(obj)

        # pathlib.Path → str
        if isinstance(obj, Path):
            return str(obj)

        # dates → ISO 8601
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()

        # enums → their value (or .name if you prefer)
        if isinstance(obj, Enum):
            return obj.value  # or obj.name

        # Let the base class raise the TypeError for the rest
        return super().default(obj)