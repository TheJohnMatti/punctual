"""Fire-and-forget notifications for job failure / quarantine / recovery
(DESIGN O10, IDEAS §2).

A sink is ``async def send(uri: str, event: dict) -> None`` (a plain ``def`` is
fine too — it's awaited if it returns a coroutine). Sinks are keyed by URI
scheme. Built-ins: ``exec``, ``http`` / ``https``, ``ntfy``, ``slack``,
``discord``. Third parties add more via ``punctual.sinks`` entry points::

    [project.entry-points."punctual.sinks"]
    teams = "punctual_teams:send"

``load_sinks()`` runs at daemon start; a plugin that fails to import is logged
and skipped (its scheme just won't resolve). ``check()`` flags configured URIs
whose scheme has no sink. A sink that errors or times out at send time is
logged and dropped — notifications never affect scheduling.
"""

from __future__ import annotations

import asyncio
import inspect
import json
import logging
import shlex
import urllib.error
import urllib.request
from collections.abc import Awaitable, Callable, Iterable
from importlib.metadata import entry_points
from typing import Any

log = logging.getLogger("punctual.notify")

_TIMEOUT = 10.0

Sink = Callable[[str, dict[str, Any]], Awaitable[None] | None]

_registry: dict[str, Sink] = {}
_inflight: set[asyncio.Task[None]] = set()


def _scheme(uri: str) -> str:
    return uri.split(":", 1)[0].lower()


def _rest(uri: str) -> str:
    """The URI with its ``scheme:`` and any leading ``//`` stripped."""
    body = uri.split(":", 1)[1] if ":" in uri else uri
    return body[2:] if body.startswith("//") else body


# --- registry --------------------------------------------------------
def load_sinks() -> None:
    """(Re)build the scheme -> sink map: built-ins, then `punctual.sinks` plugins
    (which may override a built-in)."""
    _registry.clear()
    _registry.update(_BUILTIN)
    for ep in entry_points(group="punctual.sinks"):
        try:
            _registry[ep.name.lower()] = ep.load()
        except Exception as e:  # a broken plugin must not stop the daemon
            log.warning("notify: plugin sink %r failed to load: %s", ep.name, e)
    log.debug("notify: %d sink scheme(s) available: %s", len(_registry), sorted(_registry))


def check(uris: Iterable[str | None]) -> dict[str, str]:
    """{uri: reason} for every configured URI whose scheme has no sink."""
    if not _registry:
        load_sinks()
    problems = {}
    for uri in uris:
        if uri and _scheme(uri) not in _registry:
            problems[uri] = f"no sink for scheme {_scheme(uri)!r}"
    return problems


# --- dispatch --------------------------------------------------------
async def send(uri: str, event: dict[str, Any]) -> None:
    if not _registry:
        load_sinks()
    sink = _registry.get(_scheme(uri))
    if sink is None:
        log.error("notify: no sink for %r", uri)
        return
    try:
        result = sink(uri, event)
        if inspect.isawaitable(result):
            await asyncio.wait_for(result, _TIMEOUT + 5)
    except Exception as e:
        log.warning("notify %s failed: %s", _scheme(uri), e)


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


# --- built-in sinks -------------------------------------------------
def _message(event: dict[str, Any]) -> str:
    job = event.get("job", "?")
    reason = event.get("reason") or event.get("event", "notification")
    return f"punctual: {job} — {reason}"


async def _post_json(url: str, body: dict[str, Any], headers: dict[str, str] | None = None) -> None:
    req = urllib.request.Request(
        url,
        data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json", "User-Agent": "punctual", **(headers or {})},
        method="POST",
    )

    def _do() -> None:
        try:
            with urllib.request.urlopen(req, timeout=_TIMEOUT) as r:
                if r.status >= 400:
                    raise RuntimeError(f"HTTP {r.status}")
        except urllib.error.URLError as e:
            raise RuntimeError(str(e)) from e

    await asyncio.to_thread(_do)


async def _sink_exec(uri: str, event: dict[str, Any]) -> None:
    blob = json.dumps(event)
    argv = [
        part.format(job=event.get("job", ""), reason=event.get("reason", ""), event=blob)
        for part in shlex.split(uri.split(":", 1)[1])
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


async def _sink_http(uri: str, event: dict[str, Any]) -> None:
    await _post_json(uri, event)  # the event dict verbatim


async def _sink_ntfy(uri: str, event: dict[str, Any]) -> None:
    # ntfy://topic  ->  https://ntfy.sh/topic
    # ntfy://host/topic  ->  https://host/topic
    rest = _rest(uri)
    url = f"https://ntfy.sh/{rest}" if "/" not in rest else f"https://{rest}"
    prio = "high" if event.get("event") in {"quarantine", "fail"} else "default"
    req = urllib.request.Request(
        url,
        data=_message(event).encode(),
        headers={
            "User-Agent": "punctual",
            "Title": f"punctual {event.get('event', '')}".strip(),
            "Priority": prio,
            "Tags": "punctual",
        },
        method="POST",
    )

    def _do() -> None:
        with urllib.request.urlopen(req, timeout=_TIMEOUT) as r:
            if r.status >= 400:
                raise RuntimeError(f"HTTP {r.status}")

    await asyncio.to_thread(_do)


async def _sink_slack(uri: str, event: dict[str, Any]) -> None:
    # slack://T00/B00/xxxx  ->  https://hooks.slack.com/services/T00/B00/xxxx
    await _post_json(f"https://hooks.slack.com/services/{_rest(uri)}", {"text": _message(event)})


async def _sink_discord(uri: str, event: dict[str, Any]) -> None:
    # discord://id/token  ->  https://discord.com/api/webhooks/id/token
    await _post_json(f"https://discord.com/api/webhooks/{_rest(uri)}", {"content": _message(event)})


_BUILTIN: dict[str, Sink] = {
    "exec": _sink_exec,
    "http": _sink_http,
    "https": _sink_http,
    "ntfy": _sink_ntfy,
    "slack": _sink_slack,
    "discord": _sink_discord,
}
