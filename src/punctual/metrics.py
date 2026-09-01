"""Prometheus text exposition, hand-rolled (DESIGN O6, M3 slice 1).

No `prometheus_client` — most of these are *derived from the store* on each
scrape (counts, time-since-last-success), not counters incremented in-process,
so a client library barely fits and would cost a dependency (D7).

`punctual metrics` dumps this over the control socket; `/metrics` serves it over
HTTP when `[observability] metrics_port` is set.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import AsyncIterator, Callable
from datetime import UTC, datetime

from punctual.models import Job
from punctual.store import Store

log = logging.getLogger("punctual.metrics")

# run_duration histogram buckets (seconds) — cron-job shaped, sub-second to an hour
_BUCKETS = (0.5, 1, 2, 5, 10, 30, 60, 120, 300, 900, 1800, 3600)


def _esc(v: str) -> str:
    return v.replace("\\", "\\\\").replace('"', '\\"')


class _Out:
    def __init__(self) -> None:
        self._lines: list[str] = []

    def metric(self, name: str, kind: str, help_: str) -> None:
        self._lines.append(f"# HELP {name} {help_}")
        self._lines.append(f"# TYPE {name} {kind}")

    def sample(self, name: str, value: float, **labels: str) -> None:
        lbl = ",".join(f'{k}="{_esc(v)}"' for k, v in sorted(labels.items()))
        self._lines.append(f"{name}{{{lbl}}} {value:g}" if lbl else f"{name} {value:g}")

    def text(self) -> str:
        return "\n".join(self._lines) + "\n"


def render(store: Store, jobs: list[Job], *, loop_lag: float | None = None) -> str:
    now = datetime.now(UTC)
    snap = store.metrics_snapshot()
    out = _Out()

    out.metric("punctual_up", "gauge", "1 when the scheduler is serving metrics")
    out.sample("punctual_up", 1)

    out.metric("punctual_runs_total", "counter", "Finished runs by job and terminal state")
    for (jname, state), n in sorted(snap.run_counts.items()):
        out.sample("punctual_runs_total", n, job=jname, state=state)

    out.metric("punctual_lost_runs_total", "counter", "Runs lost mid-flight (subset of runs_total)")
    for j in jobs:
        out.sample("punctual_lost_runs_total", snap.run_counts.get((j.name, "lost"), 0), job=j.name)

    out.metric(
        "punctual_time_since_last_success_seconds",
        "gauge",
        "Age of each job's most recent success (the SLO signal)",
    )
    for j in jobs:
        last = snap.last_success.get(j.name)
        if last is not None:
            out.sample(
                "punctual_time_since_last_success_seconds", (now - last).total_seconds(), job=j.name
            )

    out.metric("punctual_job_quarantined", "gauge", "1 if the job is quarantined")
    out.metric("punctual_job_consecutive_failures", "gauge", "Consecutive failed fires")
    for j in jobs:
        st = store.job_state(j.name)
        out.sample("punctual_job_quarantined", int(st.quarantined), job=j.name)
        out.sample("punctual_job_consecutive_failures", st.consecutive_failures, job=j.name)

    _histogram(out, snap.durations)

    out.metric("punctual_pending_retries", "gauge", "Retries waiting on their backoff")
    out.sample("punctual_pending_retries", snap.pending_retries)

    if loop_lag is not None:
        out.metric(
            "punctual_scheduler_loop_lag_seconds",
            "gauge",
            "How late the last tick woke vs its scheduled time",
        )
        out.sample("punctual_scheduler_loop_lag_seconds", max(0.0, loop_lag))

    return out.text()


@contextlib.asynccontextmanager
async def http_server(
    addr: str,
    port: int,
    metrics_text: Callable[[], str],
    healthz: Callable[[], tuple[bool, str]],
) -> AsyncIterator[asyncio.AbstractServer]:
    """A minimal HTTP/1.1 listener: GET /metrics and GET /healthz. stdlib only."""

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            request_line = await asyncio.wait_for(reader.readline(), timeout=5)
            parts = request_line.decode("latin-1").split(" ")
            path = parts[1] if len(parts) > 1 else "/"
            while (await reader.readline()).strip():  # consume headers
                pass
            if path.startswith("/metrics"):
                body, status, ctype = metrics_text().encode(), "200 OK", "text/plain; version=0.0.4"
            elif path.startswith("/healthz"):
                ok, reason = healthz()
                status = "200 OK" if ok else "503 Service Unavailable"
                body, ctype = (reason + "\n").encode(), "text/plain"
            else:
                body, status, ctype = b"not found\n", "404 Not Found", "text/plain"
            writer.write(
                f"HTTP/1.1 {status}\r\nContent-Type: {ctype}\r\n"
                f"Content-Length: {len(body)}\r\nConnection: close\r\n\r\n".encode()
                + body
            )
            await writer.drain()
        except (TimeoutError, ConnectionError, UnicodeDecodeError):
            pass  # a slow / broken / garbage client is not our problem — just drop it
        finally:
            writer.close()

    server = await asyncio.start_server(handle, addr, port)
    log.info("metrics on http://%s:%d/metrics", addr, port)
    try:
        yield server
    finally:
        server.close()
        await server.wait_closed()


def _histogram(out: _Out, durations: list[tuple[str, float]]) -> None:
    if not durations:
        return
    out.metric("punctual_run_duration_seconds", "histogram", "Run wall-clock duration")
    by_job: dict[str, list[float]] = {}
    for job, secs in durations:
        by_job.setdefault(job, []).append(secs)
    for job, secs_list in sorted(by_job.items()):
        cumulative = 0
        for edge in _BUCKETS:
            cumulative = sum(1 for s in secs_list if s <= edge)
            out.sample("punctual_run_duration_seconds_bucket", cumulative, job=job, le=str(edge))
        out.sample("punctual_run_duration_seconds_bucket", len(secs_list), job=job, le="+Inf")
        out.sample("punctual_run_duration_seconds_sum", sum(secs_list), job=job)
        out.sample("punctual_run_duration_seconds_count", len(secs_list), job=job)
