"""The daemon (DESIGN D2). Owns the asyncio loop, decides what fires when,
claims each fire exactly once, hands it to the executor, records the result.

M1 slice 1 — "it fires": schedule-from-now, single fire per job per tick
(run_latest semantics for a slow loop), subprocess exec + output tail, durable
run records. Deliberately *not* here yet:

  - cross-restart catch-up from job_clock (DESIGN O3) — next slice
  - LOST detection / adopt-or-kill on restart (DESIGN O2b) — next slice
  - retries / backoff / quarantine (DESIGN M2)
  - hard-kill on second signal (`stop --kill`) — next slice
"""

from __future__ import annotations

import asyncio
import contextlib
import signal
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import UTC, datetime

from punctual.executor import execute
from punctual.models import Job, Run, RunState
from punctual.schedule import next_fire
from punctual.store import Store

# Cap between loop wakes so a newly-due job, a signal, or (later) a config
# reload is picked up within a bounded delay even when the next fire is hours off.
MAX_SLEEP = 60.0


@dataclass(slots=True)
class Scheduler:
    jobs: list[Job]
    store: Store
    instance_id: str  # this daemon's identity (fencing token later)
    handle_signals: bool = True  # tests set False; the CLI leaves it on

    _stopping: asyncio.Event = field(default_factory=asyncio.Event, init=False)
    _wake: asyncio.Event = field(default_factory=asyncio.Event, init=False)
    _inflight: dict[str, int] = field(default_factory=lambda: defaultdict(int), init=False)
    _since: dict[str, datetime] = field(default_factory=dict, init=False)

    async def run(self) -> None:
        self._install_signal_handlers()
        try:
            await self._recover()

            started = datetime.now(UTC)
            for job in self.jobs:
                self._since.setdefault(job.name, started)

            async with asyncio.TaskGroup() as tg:
                while not self._stopping.is_set():
                    self._dispatch_due(tg)
                    await self._sleep(self._time_to_next())
            # exiting the TaskGroup waits for in-flight runs to finish (drain)
        finally:
            self._remove_signal_handlers()

    # --- scheduling -------------------------------------------------------
    def _enabled(self) -> list[Job]:
        return [j for j in self.jobs if j.enabled]

    def _dispatch_due(self, tg: asyncio.TaskGroup) -> None:
        now = datetime.now(UTC)
        for job in self._enabled():
            due = self._latest_due(job, now)
            if due is None:
                continue
            self._since[job.name] = due
            if self._inflight[job.name] >= job.concurrency:
                # a prior run of this job is still going; slice 1 drops the fire
                # rather than queueing it. TODO M2: record SKIPPED + metric.
                continue
            run = self.store.claim(job.name, due, self.instance_id)
            if run is None:
                continue  # already claimed (another daemon, or a restart)
            tg.create_task(self._execute_run(job, run))

    def _latest_due(self, job: Job, now: datetime) -> datetime | None:
        """Most recent fire in ``(_since[job], now]``.

        Slice 1 collapses a backlog to its last entry — run_latest semantics — so
        a slow loop or a laptop suspend can't stampede. Honouring ``on_missed``
        (skip / run_each) and cross-restart catch-up (O3) is a later slice.
        """
        cursor = next_fire(job.schedule, self._since[job.name], job.timezone)
        due: datetime | None = None
        while cursor <= now:
            due = cursor
            cursor = next_fire(job.schedule, cursor, job.timezone)
        return due

    def _time_to_next(self) -> float:
        now = datetime.now(UTC)
        fires = [next_fire(j.schedule, now, j.timezone) for j in self._enabled()]
        if not fires:
            return MAX_SLEEP
        return max(0.0, (min(fires) - now).total_seconds())

    async def _sleep(self, seconds: float) -> None:
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(self._wake.wait(), timeout=min(seconds, MAX_SLEEP))
        self._wake.clear()

    # --- execution -------------------------------------------------------
    async def _execute_run(self, job: Job, run: Run) -> None:
        self._inflight[job.name] += 1
        try:
            run.started_at = datetime.now(UTC)
            run.transition_to(RunState.RUNNING)
            self.store.mark(run)

            outcome = await execute(job, run, timeout=job.timeout)

            run.finished_at = datetime.now(UTC)
            run.exit_code = outcome.exit_code
            run.stdout_tail = outcome.stdout_tail.decode("utf-8", "replace")
            run.stderr_tail = outcome.stderr_tail.decode("utf-8", "replace")
            if outcome.timed_out:
                run.transition_to(RunState.TIMED_OUT)
            elif outcome.exit_code == 0:
                run.transition_to(RunState.SUCCEEDED)
            else:
                run.transition_to(RunState.FAILED)
            self.store.mark(run)
            self.store.set_last_fire(job.name, run.scheduled_for)
        finally:
            self._inflight[job.name] -= 1

    async def _recover(self) -> None:
        """Runs left non-terminal by a dead daemon -> LOST, then re-decide.

        Slice 1: no-op. The O2b recovery (adopt idempotent / kill-orphan
        non-idempotent / resolve-from-sentinel) lands with catch-up.
        """

    # --- shutdown -------------------------------------------------------
    def _install_signal_handlers(self) -> None:
        if not self.handle_signals:
            return
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            with contextlib.suppress(NotImplementedError):  # not on Windows
                loop.add_signal_handler(sig, self._request_stop)

    def _remove_signal_handlers(self) -> None:
        if not self.handle_signals:
            return
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            with contextlib.suppress(NotImplementedError, ValueError):
                loop.remove_signal_handler(sig)

    def _request_stop(self) -> None:
        self._stopping.set()
        self._wake.set()


async def serve(jobs: list[Job], store: Store, instance_id: str) -> None:
    await Scheduler(jobs=jobs, store=store, instance_id=instance_id).run()
