# logging_setup.py
import json, logging, os, socket, sys, time
from typing import Mapping
from logging import Logger

class JSONFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        # base fields (stable across all logs)
        entry = {
            "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(record.created)),
            "level": record.levelname,
            "logger": record.name,
            "service": os.getenv("SERVICE_NAME", "OdinBackupApp"),
            "host": socket.gethostname(),
            "msg": record.getMessage(),
        }
        # include exception text if present
        if record.exc_info:
            entry["exc"] = self.formatException(record.exc_info)

        # include any custom extras passed via LoggerAdapter or log(..., extra={})
        for k, v in record.__dict__.items():
            if k not in (
                "name","msg","args","levelname","levelno","pathname","filename",
                "module","exc_info","exc_text","stack_info","lineno","funcName",
                "created","msecs","relativeCreated","thread","threadName",
                "processName","process"
            ):
                entry[k] = v
        return json.dumps(entry, ensure_ascii=False)

def setup_logging(level: str | None = "INFO", appName: str = "OdinBackup") -> logging.Logger:
    """
    Configure root logger to write structured JSON to stdout.
    Keeps things agent/tee-friendly.
    """
    lvl = (level or os.getenv("LOG_LEVEL", "INFO")).upper()

    root = logging.getLogger()
    root.setLevel(lvl)
    root.handlers.clear()

    h = logging.StreamHandler(sys.stdout)
    h.setLevel(lvl)
    h.setFormatter(JSONFormatter())
    root.addHandler(h)

    # convenience: return a namespaced logger for app code
    return logging.getLogger(appName)

class WithContext(logging.LoggerAdapter):
    """
    Lightweight context injector.
    Use: log = WithContext(logging.getLogger("app.task"), {"run_id": rid})
    """
    def process(self, msg, kwargs):
        ctx = self.extra or {}
        # allow passing extra per-call and merging
        kw_extra: Mapping = kwargs.get("extra", {})
        merged = {**ctx, **kw_extra}
        kwargs["extra"] = merged
        return msg, kwargs
