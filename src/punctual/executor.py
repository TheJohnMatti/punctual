"""Run one job's command as a subprocess and report how it went.

DESIGN D5 (subprocess first, argv not shell), O4 (idempotency token in the
child env), O5 (output capture — tail ring-buffer; full-stream-to-file comes
later), O2 (RUNNING -> terminal transitions live in the scheduler; this module
just produces the raw :class:`Outcome`).

Every job runs in its **own session / process group** (`start_new_session`), so
a timeout or a `stop --kill` can take down the whole tree (a shell wrapper and
its children), not just the direct child. Whether a job *survives* the daemon is
a separate question, settled on restart by O2b recovery, not here.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import signal
import sys
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path

from punctual.models import Job, Run

# O5: how much stdout/stderr we keep. The tail is what `punctual history` shows;
# a full stream-to-file lands in a later slice.
TAIL_BYTES = 16 * 1024

# How long a process group gets after SIGTERM before it gets SIGKILL.
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


def _child_env(job: Job, run: Run, extra: Mapping[str, str] | None = None) -> dict[str, str]:
    env = dict(os.environ)
    env.update(job.env)
    if extra:  # e.g. the store's reconnect vars, so `step()` can reach it (M6)
        env.update(extra)
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


async def execute(
    job: Job,
    run: Run,
    *,
    timeout: timedelta | None,
    on_spawn: Callable[[int], None] | None = None,
    run_dir: Path | None = None,
    env: Mapping[str, str] | None = None,
) -> Outcome:
    """Spawn ``job.command``, capture the output tail, enforce ``timeout``.

    ``on_spawn`` is called with the pid the moment it exists, so the scheduler
    can persist it for restart recovery (O2b). When ``run_dir`` is given the
    command runs under the ``punctual._runner`` sentinel wrapper, which writes
    ``<run_dir>/exit`` so a lost run can be resolved to its real outcome.

    Never raises for a non-zero exit — that is a normal :class:`Outcome`. A
    command that cannot be spawned at all (not found, not executable) comes back
    as exit code 127 with the reason on stderr, matching a shell. On cancellation
    (a `stop --kill`) the process group is killed before the exception propagates.
    """
    if run_dir is not None:
        run_dir.mkdir(parents=True, exist_ok=True)
        argv = [sys.executable, "-m", "punctual._runner", str(run_dir), "--", *job.command]
    else:
        argv = list(job.command)
    try:
        proc = await asyncio.create_subprocess_exec(
            *argv,
            cwd=job.workdir,
            env=_child_env(job, run, env),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            start_new_session=True,  # own process group -> group-wide kill
        )
    except (FileNotFoundError, PermissionError, NotADirectoryError) as e:
        return Outcome(exit_code=127, timed_out=False, stdout_tail=b"", stderr_tail=str(e).encode())

    if on_spawn is not None:
        on_spawn(proc.pid)

    out, err = _Ring(TAIL_BYTES), _Ring(TAIL_BYTES)
    timed_out = False
    try:
        try:
            async with asyncio.timeout(timeout.total_seconds() if timeout else None):
                await asyncio.gather(
                    proc.wait(),
                    _drain(proc.stdout, out),
                    _drain(proc.stderr, err),
                )
        except TimeoutError:
            timed_out = True
            await _kill(proc)
            # process is gone; flush whatever it left in the pipes
            await asyncio.gather(_drain(proc.stdout, out), _drain(proc.stderr, err))
    except asyncio.CancelledError:
        await _kill(proc)
        raise

    return Outcome(
        exit_code=proc.returncode if proc.returncode is not None else -1,
        timed_out=timed_out,
        stdout_tail=out.bytes(),
        stderr_tail=err.bytes(),
    )


@dataclass(slots=True)
class Sentinel:
    exit_code: int
    signaled: bool


def read_sentinel(run_dir: Path) -> Sentinel | None:
    """The outcome the `_runner` wrapper recorded, or None if it never got that
    far (no file, or a half-written one)."""
    try:
        data = json.loads((run_dir / "exit").read_text())
        return Sentinel(exit_code=int(data["code"]), signaled=bool(data["signaled"]))
    except (OSError, ValueError, KeyError):
        return None


def kill_group(pid: int, sig: int = signal.SIGKILL) -> None:
    """Signal the whole process group led by ``pid``. Best effort — a pid that
    is already gone (or was never ours) is not an error."""
    with contextlib.suppress(ProcessLookupError, PermissionError):
        os.killpg(os.getpgid(pid), sig)


async def _kill(proc: asyncio.subprocess.Process) -> None:
    """SIGTERM the whole process group, then SIGKILL it after a grace period."""
    if proc.returncode is not None:
        return
    kill_group(proc.pid, signal.SIGTERM)
    try:
        async with asyncio.timeout(_KILL_GRACE.total_seconds()):
            await proc.wait()
    except TimeoutError:
        kill_group(proc.pid, signal.SIGKILL)
        await proc.wait()
