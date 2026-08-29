"""Run one job's command as a subprocess and report how it went.

DESIGN D5 (subprocess first), O5 (output capture — ring-buffer + file, sizes TBD),
O2 (RUNNING -> terminal transitions).

STUB — signatures are stable, body lands in M1.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta

from punctual.models import Job, Run


@dataclass(slots=True)
class Outcome:
    exit_code: int
    timed_out: bool
    stdout_tail: bytes
    stderr_tail: bytes


async def execute(job: Job, run: Run, *, timeout: timedelta | None) -> Outcome:
    """Spawn job.command, stream+capture output, enforce timeout, return Outcome.

    M1 checklist:
      - asyncio.create_subprocess_exec(*job.command, cwd=job.workdir, env=...)
      - stream stdout/stderr concurrently; ring-buffer the tail (DESIGN O5)
      - PUNCTUAL_RUN_ID / PUNCTUAL_SCHEDULED_FOR in the child env (DESIGN O4)
      - on timeout: SIGTERM, grace period, SIGKILL; Outcome(timed_out=True)
      - never raise for a non-zero exit; that's a normal Outcome
    """
    raise NotImplementedError("executor.execute lands in M1")
