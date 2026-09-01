"""The daemon (DESIGN D2). Owns the asyncio loop, decides what fires when,
claims each fire exactly once, hands it to the executor, records the result.

On start it (1) **recovers** runs a dead daemon left mid-flight (O2b) and
(2) **catches up** fires that came due while it was down (O3), per each job's
``on_missed``. Then the tick loop takes over.

Failed / timed-out runs are retried per the job's ``RetryPolicy`` (M2): a
durable RETRYING row with a ``not_before`` timestamp, swept by the tick loop
when its backoff elapses — so a retry pending across a restart isn't lost.
Every run is wrapped by ``punctual._runner``, which records an exit sentinel so
a run the daemon lost mid-flight resolves to its real outcome, not a blind LOST.

A job that fails `quarantine_after` fires in a row is **quarantined** (O9): its
fires are skipped until `punctual resume` or a `quarantine_cooldown` probe.
`on_fail` / `on_quarantine` URIs are dispatched fire-and-forget (O10).

Shutdown: the first SIGINT/SIGTERM is a **drain** — stop claiming, let in-flight
runs finish, exit. A second signal is **stop --kill** — SIGKILL every in-flight
job's process group; those runs land FAILED.

Deliberately *not* here yet: `punctual why` (M2 slice 3), the control socket for
`punctual drain` / `stop` / `reload` (M2 slice 4).
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import shutil
import signal
from collections import defaultdict
from collections.abc import Callable
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta

from punctual import notify
from punctual.control import ControlServer
from punctual.executor import Sentinel, execute, kill_group, read_sentinel
from punctual.models import (
    FAILURE_OUTCOMES,
    RETRYABLE_OUTCOMES,
    InvalidTransition,
    Job,
    JobState,
    MissedPolicy,
    OnLost,
    Run,
    RunState,
)
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
    control: bool = False  # control socket (M2 slice 4) — `serve()` turns it on
    config_reload: Callable[[], list[Job]] | None = None  # re-parse punctual.toml

    _stopping: asyncio.Event = field(default_factory=asyncio.Event, init=False)
    _wake: asyncio.Event = field(default_factory=asyncio.Event, init=False)
    _inflight: dict[str, int] = field(default_factory=lambda: defaultdict(int), init=False)
    _since: dict[str, datetime] = field(default_factory=dict, init=False)
    _running: dict[int, Run] = field(default_factory=dict, init=False)  # run id -> Run
    _started: datetime = field(default_factory=lambda: datetime.now(UTC), init=False)

    async def run(self) -> None:
        self._install_signal_handlers()
        try:
            self._started = datetime.now(UTC)
            for job in self.jobs:
                self._since.setdefault(job.name, self._started)

            async with self._control_server(), asyncio.TaskGroup() as tg:
                self._recover(tg)
                for job in self._enabled():
                    fires = self._plan_catch_up(job)
                    if fires:
                        tg.create_task(self._run_sequence(job, fires))

                while not self._stopping.is_set():
                    self._dispatch_due(tg)
                    self._sweep_retries(tg)
                    await self._sleep(self._time_to_next())
            # exiting the TaskGroup waits for in-flight runs to finish (drain)
            await notify.drain()
        finally:
            self._remove_signal_handlers()

    def _control_server(self) -> AbstractAsyncContextManager[object]:
        if not self.control:
            return contextlib.nullcontext()
        return ControlServer(self)

    # --- driven from the control socket / signals --------------------
    def request_drain(self) -> None:
        self._stopping.set()
        self._wake.set()

    def request_kill(self) -> None:
        log.warning("stop --kill — SIGKILL %d in-flight job(s)", len(self._running))
        for run in list(self._running.values()):
            if run.pid is not None:
                kill_group(run.pid)

    def in_flight(self) -> int:
        return len(self._running)

    def control_status(self) -> dict[str, object]:
        import os

        return {
            "pid": os.getpid(),
            "jobs": len(self._enabled()),
            "uptime_s": round((datetime.now(UTC) - self._started).total_seconds()),
            "in_flight": self.in_flight(),
        }

    def reload(self) -> dict[str, object]:
        if self.config_reload is None:
            return {"ok": False, "error": "daemon has no config path to reload"}
        try:
            new = self.config_reload()
        except Exception as e:  # bad config — keep running the old one
            return {"ok": False, "error": f"config invalid, not applied: {e}"}
        old_by = {j.name: j for j in self.jobs}
        new_by = {j.name: j for j in new}
        added = sorted(new_by.keys() - old_by.keys())
        removed = sorted(old_by.keys() - new_by.keys())
        changed = sorted(n for n in old_by.keys() & new_by.keys() if old_by[n] != new_by[n])

        self.jobs = [j for n, j in old_by.items() if n not in removed] + [new_by[n] for n in added]
        now = datetime.now(UTC)
        for n in added:
            self._since.setdefault(n, now)
        for n in removed:
            self._since.pop(n, None)
        self._wake.set()
        log.info("reload: added=%s removed=%s changed=%s", added, removed, changed)
        note = "changed jobs keep their old definition — restart to apply" if changed else ""
        return {"added": added, "removed": removed, "changed": changed, "note": note}

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
            if run.state is RunState.RETRYING:
                continue  # durable — the retry sweep will pick it up on schedule
            if run.state is RunState.CLAIMED:
                # nothing was spawned — the fire is safe to run now, as-is.
                log.info("resuming CLAIMED %s run %s (nothing had spawned)", run.job, run.id)
                self._launch(job, run, tg)
            elif run.state is RunState.RUNNING:
                self._recover_running(job, run, tg)
            self.store.set_last_fire(run.job, run.scheduled_for)
            self._advance_since(run.job, run.scheduled_for)

    def _recover_running(self, job: Job, run: Run, tg: asyncio.TaskGroup) -> None:
        alive = run.pid is not None and identity_matches(run.pid, run.pid_start_time)

        if not alive and run.id is not None:
            # the wrapper may have recorded the real outcome before the daemon died
            sentinel = read_sentinel(self.store.run_dir(run.id))
            if sentinel is not None:
                self._resolve_from_sentinel(job, run, sentinel)
                return

        if alive:
            if job.idempotent:
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
                kill_group(run.pid)  # type: ignore[arg-type]

        self._to_lost(run, note="daemon died mid-flight, no sentinel")
        if job.effective_on_lost is OnLost.RETRY:
            self._schedule_retry(job, run, delay=timedelta(0))
        else:
            log.error("LOST %s run %s — on_lost=fail, needs a human", run.job, run.id)
            self._update_health(job, run, RunState.LOST)

    def _resolve_from_sentinel(self, job: Job, run: Run, sentinel: Sentinel) -> None:
        run.finished_at = datetime.now(UTC)
        run.exit_code = sentinel.exit_code
        if sentinel.exit_code == 0:
            outcome = RunState.SUCCEEDED
        elif sentinel.signaled:
            outcome = RunState.TIMED_OUT  # killed while unsupervised — treat as a timeout
        else:
            outcome = RunState.FAILED
        run.transition_to(outcome)
        self.store.mark(run)
        log.info(
            "recovered_from_sentinel: %s run %s -> %s (exit %s)",
            run.job,
            run.id,
            outcome.value,
            sentinel.exit_code,
        )
        self._after_terminal(job, run, outcome)

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
        if self.store.job_state(job.name).quarantined:
            self._since[job.name] = now  # a quarantined job doesn't backfill
            return []
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

            state = self.store.job_state(job.name)
            if state.quarantined and not self._quarantine_lets_fire_through(job, state, now):
                state.skipped_quarantined += 1
                self.store.save_job_state(state)
                self.store.set_last_fire(job.name, due)  # no catch-up storm on resume
                continue

            if self._inflight[job.name] >= job.concurrency:
                # a prior run of this job is still going; drop the fire rather
                # than queue it. TODO M2 slice 3: record SKIPPED + metric.
                continue
            run = self.store.claim(job.name, due, self.instance_id)
            if run is None:
                continue  # already claimed (another daemon, or recovery)
            self._launch(job, run, tg)

    def _quarantine_lets_fire_through(self, job: Job, state: JobState, now: datetime) -> bool:
        """Despite quarantine, run this fire? — an operator resume, or a cooldown probe."""
        if state.resume_requested:
            log.info("%s: resume requested by operator — quarantine cleared", job.name)
            self._clear_quarantine(state)
            self.store.save_job_state(state)
            return True
        if (
            job.quarantine_cooldown is not None
            and state.quarantined_at is not None
            and now - state.quarantined_at >= job.quarantine_cooldown
            and self._inflight[job.name] == 0
        ):
            log.info("%s: cooldown elapsed — letting one probe fire through", job.name)
            return True
        return False

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
        candidates = [next_fire(j.schedule, now, j.timezone) for j in self._enabled()]
        if (retry_at := self.store.next_retry_at()) is not None:
            candidates.append(retry_at)
        if not candidates:
            return MAX_SLEEP
        return max(0.0, (min(candidates) - now).total_seconds())

    async def _sleep(self, seconds: float) -> None:
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(self._wake.wait(), timeout=min(seconds, MAX_SLEEP))
        self._wake.clear()

    # --- retries (M2) ----------------------------------------------
    def _sweep_retries(self, tg: asyncio.TaskGroup) -> None:
        for run in self.store.due_retries(datetime.now(UTC)):
            job = self._job(run.job)
            if job is None or not job.enabled:
                continue
            if self.store.job_state(job.name).quarantined:
                continue  # job is out — its pending retry waits too
            if self._inflight[job.name] >= job.concurrency:
                continue  # still busy; try again next tick
            run.transition_to(RunState.CLAIMED)  # RETRYING -> CLAIMED
            run.not_before = None
            self.store.mark(run)
            self._launch(job, run, tg)

    def _schedule_retry(self, job: Job, run: Run, delay: timedelta) -> None:
        not_before = datetime.now(UTC) + delay
        retry = self.store.schedule_retry(
            run.job, run.scheduled_for, run.attempt + 1, not_before, self.instance_id
        )
        if retry is not None:
            log.info(
                "%s run %s (%s) -> retry attempt %d, not before %s",
                run.job,
                run.id,
                run.state.value,
                retry.attempt,
                not_before.isoformat(timespec="seconds"),
            )
            self._wake.set()  # make the loop re-evaluate its sleep

    def _after_terminal(self, job: Job, run: Run, outcome: RunState) -> None:
        self.store.set_last_fire(job.name, run.scheduled_for)
        retryable = outcome in RETRYABLE_OUTCOMES
        if retryable and run.attempt <= job.retries.max:
            self._schedule_retry(job, run, job.retries.delay_for_attempt(run.attempt))
            return
        # this fire is now final — a success, or every retry is spent
        if retryable and job.retries.max:
            log.warning(
                "%s run %s: retries exhausted (attempt %d of %d) — %s stands",
                run.job,
                run.id,
                run.attempt,
                job.retries.max + 1,
                outcome.value,
            )
        self._cleanup_run_dir(run)
        self._update_health(job, run, outcome)

    # --- quarantine (M2 slice 2) ---------------------------------------
    def _update_health(self, job: Job, run: Run, outcome: RunState) -> None:
        state = self.store.job_state(job.name)
        if outcome in FAILURE_OUTCOMES:
            state.consecutive_failures += 1
            self._notify(job.on_fail, "fail", job, run, state, outcome)
            if state.quarantined:
                state.quarantined_at = datetime.now(UTC)  # failed probe — restart cooldown
                log.warning("%s: quarantine probe failed — still quarantined", job.name)
            elif job.quarantine_after and state.consecutive_failures >= job.quarantine_after:
                state.quarantined_at = datetime.now(UTC)
                state.quarantine_reason = (
                    f"{state.consecutive_failures} consecutive failed fires (last: {outcome.value})"
                )
                log.error(
                    "QUARANTINED %s — %s. Fires skipped until `punctual resume %s`.",
                    job.name,
                    state.quarantine_reason,
                    job.name,
                )
                self._notify(job.on_quarantine, "quarantine", job, run, state, outcome)
            self.store.save_job_state(state)
        elif state.quarantined:  # a probe succeeded
            log.info(
                "%s: recovered — quarantine cleared (%d fires had been skipped)",
                job.name,
                state.skipped_quarantined,
            )
            self._clear_quarantine(state)
            self.store.save_job_state(state)
        elif state.consecutive_failures:
            state.consecutive_failures = 0
            self.store.save_job_state(state)

    @staticmethod
    def _clear_quarantine(state: JobState) -> None:
        state.quarantined_at = None
        state.quarantine_reason = None
        state.consecutive_failures = 0
        state.skipped_quarantined = 0
        state.resume_requested = False

    def _notify(
        self, uri: str | None, event: str, job: Job, run: Run, state: JobState, outcome: RunState
    ) -> None:
        if not uri:
            return
        notify.fire(
            uri,
            {
                "event": event,
                "job": job.name,
                "reason": state.quarantine_reason or f"run {outcome.value}",
                "attempt": run.attempt,
                "scheduled_for": run.scheduled_for.isoformat(),
                "exit_code": run.exit_code,
                "consecutive_failures": state.consecutive_failures,
                "at": datetime.now(UTC).isoformat(timespec="seconds"),
            },
        )

    def _cleanup_run_dir(self, run: Run) -> None:
        if run.id is not None:
            shutil.rmtree(self.store.run_dir(run.id), ignore_errors=True)

    # --- execution --------------------------------------------------
    def _launch(self, job: Job, run: Run, tg: asyncio.TaskGroup) -> None:
        tg.create_task(self._execute_run(job, run))

    async def _execute_run(self, job: Job, run: Run) -> None:
        self._inflight[job.name] += 1
        self._running[run.id] = run  # type: ignore[index]  # id is set post-claim
        run_dir = self.store.run_dir(run.id) if run.id is not None else None
        try:
            run.started_at = datetime.now(UTC)
            run.transition_to(RunState.RUNNING)
            self.store.mark(run)

            def _record_pid(pid: int) -> None:
                run.pid = pid
                run.pid_start_time = pid_start_time(pid)
                self.store.mark(run)

            outcome = await execute(
                job, run, timeout=job.timeout, on_spawn=_record_pid, run_dir=run_dir
            )

            run.finished_at = datetime.now(UTC)
            run.exit_code = outcome.exit_code
            run.stdout_tail = outcome.stdout_tail.decode("utf-8", "replace")
            run.stderr_tail = outcome.stderr_tail.decode("utf-8", "replace")
            if outcome.timed_out:
                state = RunState.TIMED_OUT
            elif outcome.exit_code == 0:
                state = RunState.SUCCEEDED
            else:
                state = RunState.FAILED
            run.transition_to(state)
            self.store.mark(run)
            self._after_terminal(job, run, state)
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
            self.request_kill()  # second signal escalates to stop --kill
            return
        log.info("draining — no new fires; waiting for %d in-flight run(s)", len(self._running))
        self.request_drain()


async def serve(
    jobs: list[Job],
    store: Store,
    instance_id: str,
    *,
    config_reload: Callable[[], list[Job]] | None = None,
) -> None:
    await Scheduler(
        jobs=jobs,
        store=store,
        instance_id=instance_id,
        control=True,
        config_reload=config_reload,
    ).run()
