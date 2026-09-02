"""punctual — the reliability layer cron never had."""

from __future__ import annotations

from importlib.metadata import PackageNotFoundError, version

from punctual.registry import job

try:
    __version__ = version("punctual-scheduler")
except PackageNotFoundError:  # a source tree that was never installed
    __version__ = "0+unknown"

__all__ = ["__version__", "job"]
