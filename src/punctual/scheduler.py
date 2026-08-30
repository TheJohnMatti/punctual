"""The daemon (DESIGN D2). Owns the asyncio loop, decides what fires when,
claims each fire exactly once, hands it to the executor, records the result.

On start it (1) **recovers** runs a dead daemon left mid-flight (O2b) and
(2) **catches up** fires that came due while it was down (O3), per each job's
``on_missed``. Then the tick loop takes over.

Shutdown: the first SIGINT/SIGTERM is a **drain** — stop claiming, let in-flight
runs finish, exit. A second signal is **stop --kill** — SIGKILL every in-flight
job's process group; those runs land FAILED.

Deliberately *not* here yet: retries / backoff / quarantine (M2), the
exit-code sentinel that would let us resolve a lost run's real outcome (O2b),
`punctual why` (M2).
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import signal
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import UTC, datetime

from punctual.executor import execute, kill_group
from punctual.models import InvalidTransition, Job, MissedPolicy, OnLost, Run, RunState
from punctual.process import identity_matches, pid_start_time
from punctual.schedule import fires_between, next_fire
from punctual.store import Store

log = logging.getLogger("punctual.scheduler")

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
    _running: dict[int, Run] = field(default_factory=dict, init=False)  # run id -> Run

    async def run(self) -> None:
        self._install_signal_handlers()
        try:
            started = datetime.now(UTC)
            for job in self.jobs:
                self._since.setdefault(job.name, started)

            async with asyncio.TaskGroup() as tg:
                self._recover(tg)
                for job in self._enabled():
                    fires = self._plan_catch_up(job)
                    if fires:
                        tg.create_task(self._run_sequence(job, fires))

                while not self._stopping.is_set():
                    self._dispatch_due(tg)
                    await self._sleep(self._time_to_next())
            # exiting the TaskGroup waits for in-flight runs to finish (drain)
        finally:
            self._remove_signal_handlers()

    def _job(self, name: str) -> Job | None:
        return next((j for j in self.jobs if j.name == name), None)

    def _enabled(self) -> list[Job]:
        return [j for j in self.jobs if j.enabled]

    def _advance_since(self, job: str, when: datetime) -> None:
        current = self._since.get(job)
        if current is None or when > current:
            self._since[job] = when

    # --- restart recovery (O2b) -----------------------------------------
    def _recover(self, tg: asyncio.TaskGroup) -> None:
        for run in self.store.open_runs():
            job = self._job(run.job)
            if job is None:  # job removed from config — just close the record
                self._to_lost(run, note="job no longer in config")
                continue
            if run.state is RunState.CLAIMED:
                # nothing was spawned — the fire is safe to run now, as-is.
                log.info("resuming CLAIMED %s run %s (nothing had spawned)", run.job, run.id)
                self._launch(job, run, tg)
            elif run.state is RunState.RUNNING:
                self._recover_running(job, run, tg)
            self.store.set_last_fire(run.job, run.scheduled_for)
            self._advance_since(run.job, run.scheduled_for)

    def _recover_running(self, job: Job, run: Run, tg: asyncio.TaskGroup) -> None:
        if run.pid is not None and identity_matches(run.pid, run.pid_start_time):
            if job.idempotent:
                # harmless orphan; let it finish on its own, a fresh run is safe
                log.warning(
                    "orphan %s run %s (pid %s) left running — idempotent, leaving it",
                    run.job,
                    run.id,
                    run.pid,
                )
            else:
                log.warning(
                    "killing non-idempotent orphan %s run %s (pid %s)", run.job, run.id, run.pid
                )
                kill_group(run.pid)
        self._to_lost(run, note="daemon died mid-flight")
        if job.effective_on_lost is OnLost.RETRY:
            retry = self.store.claim(
                run.job, run.scheduled_for, self.instance_id, attempt=run.attempt + 1
            )
            if retry is not None:
                log.info("retrying lost %s as attempt %s", run.job, retry.attempt)
                self._launch(job, retry, tg)
        else:
            log.error("LOST %s run %s — on_lost=fail, needs a human", run.job, run.id)

    def _to_lost(self, run: Run, *, note: str) -> None:
        run.finished_at = datetime.now(UTC)
        with contextlib.suppress(InvalidTransition):
            run.transition_to(RunState.LOST)
        self.store.mark(run)
        # O6/M3: this is where lost_runs_total gets incremented.
        log.warning(
            "run %s (%s, fire %s) -> LOST: %s",
            run.id,
            run.job,
            run.scheduled_for.isoformat(timespec="minutes"),
            note,
        )

    # --- catch-up (O3) -------------------------------------------------
    def _plan_catch_up(self, job: Job) -> list[datetime]:
        """Which missed fires to replay, and advance the cursor past them now
        (so the tick loop resumes cleanly). Returns [] when there's nothing to do.
        """
        baseline = self.store.last_fire(job.name)
        now = datetime.now(UTC)
        if baseline is None:  # first run ever / brand-new job — no history to catch up
            self._since[job.name] = now
            return []
        missed = list(fires_between(job.schedule, baseline, now, job.timezone))
        if not missed:
            self._since[job.name] = baseline
            return []
        self._since[job.name] = missed[-1]

        if job.missed is MissedPolicy.SKIP:
            self.store.set_last_fire(job.name, missed[-1])
            log.info("%s: skipped %d missed fire(s) (on_missed=skip)", job.name, len(missed))
            return []
        if job.missed is MissedPolicy.RUN_LATEST:
            log.info("%s: catching up 1 of %d missed fire(s) (run_latest)", job.name, len(missed))
            return [missed[-1]]
        # RUN_EACH
        cap = job.catch_up_cap
        if cap and len(missed) > cap:
            log.warning(
                "%s: catch_up_cap=%d — replaying newest %d, DROPPING %d of %d missed fires",
                job.name,
                cap,
                cap,
                len(missed) - cap,
                len(missed),
            )
            return missed[-cap:]
        log.info("%s: replaying all %d missed fire(s) (run_each)", job.name, len(missed))
        return missed

    async def _run_sequence(self, job: Job, fires: list[datetime]) -> None:
        """Run a list of fires for one job, one at a time (respects concurrency=1
        and stops promptly on shutdown)."""
        for fire in fires:
            if self._stopping.is_set():
                return
            run = self.store.claim(job.name, fire, self.instance_id)
            if run is not None:
                await self._execute_run(job, run)

    # --- tick loop ----------------------------------------------------
    def _dispatch_due(self, tg: asyncio.TaskGroup) -> None:
        now = datetime.now(UTC)
        for job in self._enabled():
            due = self._latest_due(job, now)
            if due is None:
                continue
            self._since[job.name] = due
            if self._inflight[job.name] >= job.concurrency:
                # a prior run of this job is still going; drop the fire rather
                # than queue it. TODO M2: record SKIPPED + metric.
                continue
            run = self.store.claim(job.name, due, self.instance_id)
            if run is None:
                continue  # already claimed (another daemon, or recovery)
            self._launch(job, run, tg)

    def _latest_due(self, job: Job, now: datetime) -> datetime | None:
        """Most recent fire in ``(_since[job], now]``. Collapses a backlog to its
        last entry — run_latest semantics — so a slow loop or a laptop suspend
        can't stampede the tick loop (cross-restart catch-up is `_plan_catch_up`).
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

    # --- execution --------------------------------------------------
    def _launch(self, job: Job, run: Run, tg: asyncio.TaskGroup) -> None:
        tg.create_task(self._execute_run(job, run))

    async def _execute_run(self, job: Job, run: Run) -> None:
        self._inflight[job.name] += 1
        self._running[run.id] = run  # type: ignore[index]  # id is set post-claim
        try:
            run.started_at = datetime.now(UTC)
            run.transition_to(RunState.RUNNING)
            self.store.mark(run)

            def _record_pid(pid: int) -> None:
                run.pid = pid
                run.pid_start_time = pid_start_time(pid)
                self.store.mark(run)

            outcome = await execute(job, run, timeout=job.timeout, on_spawn=_record_pid)

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
            self._running.pop(run.id, None)  # type: ignore[arg-type]

    # --- shutdown --------------------------------------------------
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
        if self._stopping.is_set():
            # second signal: stop --kill — SIGKILL every in-flight job's group.
            # Those runs come back with a signal exit code and land as FAILED.
            log.warning("second signal — killing %d in-flight job(s)", len(self._running))
            for run in list(self._running.values()):
                if run.pid is not None:
                    kill_group(run.pid)
            return
        log.info("draining — no new fires; waiting for %d in-flight run(s)", len(self._running))
        self._stopping.set()
        self._wake.set()


async def serve(jobs: list[Job], store: Store, instance_id: str) -> None:
    await Scheduler(jobs=jobs, store=store, instance_id=instance_id).run()
