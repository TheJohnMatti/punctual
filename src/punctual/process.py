"""Tiny OS-process helpers for restart recovery (DESIGN O2b).

`pid` alone isn't a stable identity — the OS reuses pids. Where the platform
makes it cheap (Linux `/proc`), we also capture the process start time so a
recovered run can tell "my child is still alive" from "a new process now holds
that pid". Elsewhere we degrade to a pid-only liveness check.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path


def pid_alive(pid: int) -> bool:
    """True if a process with this pid currently exists."""
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True  # exists, just not ours to signal
    return True


def pid_start_time(pid: int) -> str | None:
    """An opaque, stable-for-the-life-of-the-process token, or None if this
    platform can't produce one cheaply. Compare with ``==``; don't parse."""
    if sys.platform == "linux":
        try:
            # /proc/<pid>/stat field 22 (0-based 21) is starttime in clock ticks
            # since boot. The comm field can contain spaces/parens, so split on
            # the last ')'.
            stat = Path(f"/proc/{pid}/stat").read_text()
            return stat.rsplit(")", 1)[1].split()[19]
        except (FileNotFoundError, ProcessLookupError, IndexError):
            return None
    return None


def identity_matches(pid: int, recorded_start: str | None) -> bool:
    """Is the process now holding ``pid`` the same one we recorded?

    Conservative: unknown (no recorded token, or platform can't check) counts as
    a match as long as the pid is alive — we'd rather adopt/kill our own stray
    job than wrongly declare it gone.
    """
    if not pid_alive(pid):
        return False
    if recorded_start is None:
        return True
    now = pid_start_time(pid)
    return now is None or now == recorded_start
