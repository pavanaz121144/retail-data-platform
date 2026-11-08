"""
JSON logging for the framework.

    from dataprocessorlib.utils.logger import get_json_logger
    log = get_json_logger("my.module")
    log.info("loaded", extra={"job_name": "load_raw", "row_count": 120})

Every record is one JSON line: timestamp, level, logger, message, any `extra`
fields, and the stack trace under "exception" when present. Level comes from the
`level` argument, else the `log_level` environment variable, else INFO.
"""
from __future__ import annotations

import json
import logging
import os
import sys
from datetime import datetime, timezone

_STD_ATTRS = set(logging.LogRecord("", 0, "", 0, "", (), None).__dict__) | {"message", "asctime"}


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "timestamp": datetime.fromtimestamp(record.created, timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        for key, value in record.__dict__.items():
            if key not in _STD_ATTRS and not key.startswith("_"):
                payload[key] = value
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


def get_json_logger(name: str = "dataprocessorlib", level: str | None = None, **_) -> logging.Logger:
    level = (level or os.environ.get("log_level", "INFO")).upper()
    logger = logging.getLogger(name)
    logger.setLevel(level)
    if not any(getattr(h, "_is_json", False) for h in logger.handlers):
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(JsonFormatter())
        handler._is_json = True
        logger.addHandler(handler)
    logger.propagate = False
    return logger
