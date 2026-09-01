"""Fire-and-forget notifications for job failure / quarantine (DESIGN, IDEAS §2).

Two built-in sinks, chosen by URI scheme:

  exec:<argv template>   shlex-split, ``{job}`` / ``{reason}`` / ``{event}``
                         substituted; the full event JSON is also on stdin and
                         in ``$PUNCTUAL_EVENT``. Zero deps — pipe to ntfy, mail,
                         curl, anything.
  https://… | http://…   HTTP POST, the event as a JSON body.

`ntfy://`, `slack://`, … and the entry-point plugin surface come with M3.
A sink that errors or times out is logged and dropped — notifications never
affect scheduling.
"""

from __future__ import annotations

import asyncio
import json
import logging
import shlex
import urllib.error
import urllib.request
from typing import Any

log = logging.getLogger("punctual.notify")

_TIMEOUT = 10.0


async def send(uri: str, event: dict[str, Any]) -> None:
    try:
        if uri.startswith("exec:"):
            await _exec(uri[len("exec:") :], event)
        elif uri.startswith(("http://", "https://")):
            await _webhook(uri, event)
        else:
            log.error("notify: unknown sink scheme in %r", uri)
    except Exception as e:
        log.warning("notify %s failed: %s", uri.split(":", 1)[0], e)


_inflight: set[asyncio.Task[None]] = set()


def fire(uri: str | None, event: dict[str, Any]) -> None:
    """Schedule `send` without awaiting it. Safe to call from the scheduler."""
    if not uri:
        return
    task = asyncio.get_running_loop().create_task(send(uri, event))
    _inflight.add(task)  # hold a reference so it isn't GC'd mid-flight
    task.add_done_callback(_inflight.discard)


async def drain(timeout: float = _TIMEOUT) -> None:
    """Let outstanding notifications finish (called on graceful shutdown)."""
    if _inflight:
        await asyncio.wait(set(_inflight), timeout=timeout)


async def _exec(template: str, event: dict[str, Any]) -> None:
    blob = json.dumps(event)
    argv = [
        part.format(job=event.get("job", ""), reason=event.get("reason", ""), event=blob)
        for part in shlex.split(template)
    ]
    proc = await asyncio.create_subprocess_exec(
        *argv,
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.PIPE,
        env={"PUNCTUAL_EVENT": blob},
    )
    try:
        _, err = await asyncio.wait_for(proc.communicate(blob.encode()), _TIMEOUT)
    except TimeoutError:
        proc.kill()
        raise
    if proc.returncode:
        raise RuntimeError(f"exit {proc.returncode}: {err.decode('utf-8', 'replace')[:200]}")


async def _webhook(url: str, event: dict[str, Any]) -> None:
    req = urllib.request.Request(
        url,
        data=json.dumps(event).encode(),
        headers={"Content-Type": "application/json", "User-Agent": "punctual"},
        method="POST",
    )

    def _post() -> None:
        try:
            with urllib.request.urlopen(req, timeout=_TIMEOUT) as r:
                if r.status >= 400:
                    raise RuntimeError(f"HTTP {r.status}")
        except urllib.error.URLError as e:
            raise RuntimeError(str(e)) from e

    await asyncio.to_thread(_post)
