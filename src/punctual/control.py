"""The daemon's control socket (M2 slice 4).

A Unix domain socket at ``$XDG_RUNTIME_DIR/punctual.sock`` (mode 0600), speaking
one line of JSON per request and one per reply. The CLI (`punctual drain` /
`stop` / `reload` / `ping`) is the only client today; `why` / `status` may prefer
it for live data later, and it's the seam for `punctual web`.

Commands:
  {"cmd": "ping"}              -> {"ok": true, "pid", "jobs", "uptime_s", "in_flight"}
  {"cmd": "drain"}             -> {"ok": true, "in_flight"}      (daemon exits when idle)
  {"cmd": "stop", "kill": b}   -> {"ok": true, "killed": b}
  {"cmd": "reload"}            -> {"ok": true, "added", "removed", "changed"}
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
from pathlib import Path
from typing import Any, Protocol

log = logging.getLogger("punctual.control")


class Controllable(Protocol):
    """What `ControlServer` needs from the scheduler — kept structural so this
    module doesn't import `punctual.scheduler` (no import cycle)."""

    def request_drain(self) -> None: ...
    def request_kill(self) -> None: ...
    def in_flight(self) -> int: ...
    def control_status(self) -> dict[str, object]: ...
    def reload(self) -> dict[str, object]: ...
    def trigger(self, job: str) -> dict[str, object]: ...
    def metrics_text(self) -> str: ...
    def healthz(self) -> tuple[bool, str]: ...


def socket_path() -> Path:
    if env := os.environ.get("PUNCTUAL_SOCKET"):
        return Path(env)
    base = os.environ.get("XDG_RUNTIME_DIR") or os.environ.get("TMPDIR") or "/tmp"
    return Path(base) / f"punctual-{os.getuid()}.sock"


# --- server -------------------------------------------------------------
class ControlServer:
    def __init__(self, scheduler: Controllable, path: Path | None = None) -> None:
        self._sched = scheduler
        self._path = path or socket_path()
        self._server: asyncio.Server | None = None

    async def __aenter__(self) -> ControlServer:
        if len(str(self._path).encode()) > 100:  # AF_UNIX sun_path is ~104 bytes
            raise OSError(
                f"control socket path too long ({self._path}); "
                "set $PUNCTUAL_SOCKET to something shorter"
            )
        with contextlib.suppress(FileNotFoundError):
            self._path.unlink()  # a stale socket from a crashed daemon
        self._path.parent.mkdir(parents=True, exist_ok=True)
        old_umask = os.umask(0o077)  # create the socket as 0600, no chmod race
        try:
            self._server = await asyncio.start_unix_server(self._handle, self._path)
        finally:
            os.umask(old_umask)
        log.info("control socket at %s", self._path)
        return self

    async def __aexit__(self, *_: object) -> None:
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
        with contextlib.suppress(FileNotFoundError):
            self._path.unlink()

    async def _handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            line = await asyncio.wait_for(reader.readline(), timeout=5)
            req = json.loads(line or b"{}")
            reply = await self._dispatch(req)
        except (json.JSONDecodeError, TimeoutError) as e:
            reply = {"ok": False, "error": f"bad request: {e}"}
        except Exception as e:  # a control bug must not take the daemon down
            log.exception("control command failed")
            reply = {"ok": False, "error": str(e)}
        writer.write(json.dumps(reply).encode() + b"\n")
        with contextlib.suppress(ConnectionError):
            await writer.drain()
        writer.close()

    async def _dispatch(self, req: dict[str, Any]) -> dict[str, Any]:
        cmd = req.get("cmd")
        s = self._sched
        if cmd == "ping":
            return {"ok": True, **s.control_status()}
        if cmd == "drain":
            s.request_drain()
            return {"ok": True, "in_flight": s.in_flight()}
        if cmd == "stop":
            s.request_drain()
            if req.get("kill"):
                s.request_kill()
            return {"ok": True, "killed": bool(req.get("kill"))}
        if cmd == "reload":
            return {"ok": True, **s.reload()}
        if cmd == "trigger":
            return s.trigger(str(req.get("job", "")))
        if cmd == "metrics":
            return {"ok": True, "text": s.metrics_text()}
        if cmd == "healthz":
            ok, reason = s.healthz()
            return {"ok": ok, "reason": reason}
        return {"ok": False, "error": f"unknown command {cmd!r}"}


# --- client -------------------------------------------------------------
class NotRunning(RuntimeError):
    pass


async def _request(payload: dict[str, Any], path: Path, timeout: float) -> dict[str, Any]:
    try:
        reader, writer = await asyncio.wait_for(asyncio.open_unix_connection(path), timeout=2)
    except (OSError, TimeoutError) as e:
        raise NotRunning(f"no daemon listening at {path} ({e})") from e
    try:
        writer.write(json.dumps(payload).encode() + b"\n")
        await writer.drain()
        line = await asyncio.wait_for(reader.readline(), timeout=timeout)
    finally:
        writer.close()
        with contextlib.suppress(ConnectionError):
            await writer.wait_closed()
    return json.loads(line)  # type: ignore[no-any-return]


def request(
    cmd: str, *, path: Path | None = None, timeout: float = 30.0, **kw: Any
) -> dict[str, Any]:
    """Blocking one-shot: send a command, return the reply dict. Raises
    :class:`NotRunning` if nothing is listening."""
    return asyncio.run(_request({"cmd": cmd, **kw}, path or socket_path(), timeout))


def wait_until_gone(path: Path | None = None, timeout: float = 60.0) -> bool:
    """Block until the daemon stops answering its socket. True if it went away."""
    path = path or socket_path()
    import time

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            asyncio.run(_request({"cmd": "ping"}, path, timeout=2))
        except NotRunning:
            return True
        time.sleep(0.5)
    return False
