"""Run one job's command as a subprocess and report how it went.

DESIGN D5 (subprocess first, argv not shell), O4 (idempotency token in the
child env), O5 (output capture — tail ring-buffer; full-stream-to-file comes
later), O2 (RUNNING -> terminal transitions live in the scheduler; this module
just produces the raw :class:`Outcome`).
"""

from __future__ import annotations

import asyncio
import contextlib
import os
from dataclasses import dataclass
from datetime import timedelta

from punctual.models import Job, Run

# O5: how much stdout/stderr we keep. The tail is what `punctual history` shows;
# a full stream-to-file lands in a later slice.
TAIL_BYTES = 16 * 1024

# How long a timed-out process gets after SIGTERM before we SIGKILL it.
_KILL_GRACE = timedelta(seconds=10)


@dataclass(slots=True)
class Outcome:
    exit_code: int
    timed_out: bool
    stdout_tail: bytes
    stderr_tail: bytes


class _Ring:
    """Keeps only the last ``cap`` bytes written to it."""

    __slots__ = ("_buf", "_cap")

    def __init__(self, cap: int) -> None:
        self._buf = bytearray()
        self._cap = cap

    def write(self, chunk: bytes) -> None:
        self._buf.extend(chunk)
        if len(self._buf) > self._cap:
            del self._buf[: len(self._buf) - self._cap]

    def bytes(self) -> bytes:
        return bytes(self._buf)


def _child_env(job: Job, run: Run) -> dict[str, str]:
    env = dict(os.environ)
    env.update(job.env)
    # O4: the job can use these to dedupe its own side effects.
    env["PUNCTUAL_RUN_ID"] = str(run.id)
    env["PUNCTUAL_JOB"] = job.name
    env["PUNCTUAL_SCHEDULED_FOR"] = run.scheduled_for.isoformat()
    env["PUNCTUAL_ATTEMPT"] = str(run.attempt)
    return env


async def _drain(stream: asyncio.StreamReader | None, ring: _Ring) -> None:
    if stream is None:
        return
    while chunk := await stream.read(4096):
        ring.write(chunk)


async def execute(job: Job, run: Run, *, timeout: timedelta | None) -> Outcome:
    """Spawn ``job.command``, capture the output tail, enforce ``timeout``.

    Never raises for a non-zero exit — that is a normal :class:`Outcome`. A
    command that cannot be spawned at all (not found, not executable) comes back
    as exit code 127 with the reason on stderr, matching a shell.
    """
    try:
        proc = await asyncio.create_subprocess_exec(
            *job.command,
            cwd=job.workdir,
            env=_child_env(job, run),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
    except (FileNotFoundError, PermissionError, NotADirectoryError) as e:
        return Outcome(exit_code=127, timed_out=False, stdout_tail=b"", stderr_tail=str(e).encode())

    out, err = _Ring(TAIL_BYTES), _Ring(TAIL_BYTES)
    pumps = asyncio.gather(_drain(proc.stdout, out), _drain(proc.stderr, err))

    timed_out = False
    try:
        async with asyncio.timeout(timeout.total_seconds() if timeout else None):
            await proc.wait()
    except TimeoutError:
        timed_out = True
        await _kill(proc)

    await pumps
    return Outcome(
        exit_code=proc.returncode if proc.returncode is not None else -1,
        timed_out=timed_out,
        stdout_tail=out.bytes(),
        stderr_tail=err.bytes(),
    )


async def _kill(proc: asyncio.subprocess.Process) -> None:
    """SIGTERM, then SIGKILL after a grace period.

    Slice 1 signals the direct child only. A shell wrapper that is waiting on its
    own child (`bash -lc "sleep 30; ..."`) won't exit until that child does —
    process-*group* kill lands with the O2b work in slice 2.
    """
    try:
        proc.terminate()
    except ProcessLookupError:
        return
    try:
        async with asyncio.timeout(_KILL_GRACE.total_seconds()):
            await proc.wait()
    except TimeoutError:
        with contextlib.suppress(ProcessLookupError):
            proc.kill()
        await proc.wait()
