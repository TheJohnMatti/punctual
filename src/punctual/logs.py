"""Log setup for the daemon (M3 slice 2).

`punctual run` picks the format: ``text`` (default, human / journalctl) or
``json`` (one object per line, Loki / vector / jq shaped). Structured events
attach fields via ``log.info(..., extra={"event": "...", "job": ...})``; the
JSON formatter surfaces them at the top level, the text formatter ignores them.
"""

from __future__ import annotations

import json
import logging
import time
from typing import Any

# LogRecord attributes that are noise in the JSON output (everything not below
# is treated as a caller-supplied structured field).
_STD = frozenset(
    [
        "name",
        "msg",
        "args",
        "levelname",
        "levelno",
        "pathname",
        "filename",
        "module",
        "exc_info",
        "exc_text",
        "stack_info",
        "lineno",
        "funcName",
        "created",
        "msecs",
        "relativeCreated",
        "thread",
        "threadName",
        "processName",
        "process",
        "taskName",
    ]
)


class JSONFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        out: dict[str, Any] = {
            "ts": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(record.created))
            + f".{int(record.msecs):03d}Z",
            "level": record.levelname.lower(),
            "logger": record.name,
            "msg": record.getMessage(),
        }
        for k, v in record.__dict__.items():
            if k not in _STD and not k.startswith("_"):
                out[k] = v
        if record.exc_info:
            out["exc"] = self.formatException(record.exc_info)
        return json.dumps(out, default=str)


def configure(fmt: str, *, verbose: bool) -> None:
    handler = logging.StreamHandler()
    if fmt == "json":
        handler.setFormatter(JSONFormatter())
    else:
        handler.setFormatter(
            logging.Formatter(
                "%(asctime)s %(levelname)-7s %(name)s  %(message)s",
                datefmt="%Y-%m-%dT%H:%M:%S%z",
            )
        )
    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(logging.DEBUG if verbose else logging.INFO)
