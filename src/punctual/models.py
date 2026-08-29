"""Core domain types: jobs, runs, and the run state machine.

Everything here is a plain dataclass / enum with no I/O. The store
(:mod:`punctual.store`) persists these; the scheduler and executor move runs
through the state machine.

Schema/state details are still under design — see docs/DESIGN.md O1 and O2.
"""

from __future__ import annotations

import enum
from dataclasses import dataclass, field
from datetime import datetime, timedelta


class MissedPolicy(enum.StrEnum):
    """What to do about fires that came due while the daemon was down."""

    SKIP = "skip"  # advance to the next future fire; log what was skipped
    RUN_LATEST = "run_latest"  # run once now, then resume
    RUN_EACH = "run_each"  # enqueue every missed fire (coalescing rules: DESIGN O3)


class Backoff(enum.StrEnum):
    FIXED = "fixed"
    LINEAR = "linear"
    EXPONENTIAL = "exponential"


class OnLost(enum.StrEnum):
    """What to do with a run declared LOST (DESIGN O2b).

    The default is *derived* from Job.idempotent (false -> FAIL, true -> RETRY);
    an explicit value on the job overrides that.
    """

    FAIL = "fail"  # LOST is the final record; page, don't touch
    RETRY = "retry"  # LOST -> RETRYING -> CLAIMED (new attempt)


class RunState(enum.StrEnum):
    """DESIGN O2 - proposed. Transitions enforced in Run.transition_to()."""

    PENDING = "pending"  # a fire is due; row exists, nothing spawned
    CLAIMED = "claimed"  # this daemon owns it (exactly-once boundary)
    RUNNING = "running"  # subprocess is live
    SUCCEEDED = "succeeded"
    FAILED = "failed"  # non-zero exit
    TIMED_OUT = "timed_out"  # exceeded job.timeout, killed
    LOST = "lost"  # was RUNNING when the daemon died; fate unknown
    RETRYING = "retrying"  # failed, a retry is scheduled
    SKIPPED = "skipped"  # missed + MissedPolicy.SKIP, or job disabled
    QUARANTINED = "quarantined"  # too many consecutive failures; stop, page

    @property
    def terminal(self) -> bool:
        return self in {
            RunState.SUCCEEDED,
            RunState.FAILED,
            RunState.TIMED_OUT,
            RunState.SKIPPED,
            RunState.QUARANTINED,
        }


# Allowed transitions. Anything not listed raises in Run.transition_to().
_TRANSITIONS: dict[RunState, set[RunState]] = {
    RunState.PENDING: {RunState.CLAIMED, RunState.SKIPPED},
    RunState.CLAIMED: {RunState.RUNNING, RunState.LOST},
    RunState.RUNNING: {
        RunState.SUCCEEDED,
        RunState.FAILED,
        RunState.TIMED_OUT,
        RunState.LOST,
    },
    RunState.FAILED: {RunState.RETRYING, RunState.QUARANTINED},
    RunState.TIMED_OUT: {RunState.RETRYING, RunState.QUARANTINED},
    RunState.LOST: {RunState.RETRYING, RunState.QUARANTINED, RunState.SKIPPED},
    RunState.RETRYING: {RunState.CLAIMED},
}


class InvalidTransition(RuntimeError):
    pass


@dataclass(slots=True)
class RetryPolicy:
    max: int = 0
    backoff: Backoff = Backoff.EXPONENTIAL
    base_delay: timedelta = timedelta(seconds=15)
    max_delay: timedelta = timedelta(minutes=30)

    def delay_for_attempt(self, attempt: int) -> timedelta:
        """attempt is 1-based (the delay *before* the Nth retry)."""
        if self.backoff is Backoff.FIXED:
            d = self.base_delay
        elif self.backoff is Backoff.LINEAR:
            d = self.base_delay * attempt
        else:
            d = self.base_delay * (2 ** (attempt - 1))
        return min(d, self.max_delay)


@dataclass(slots=True)
class Job:
    """A job definition, as parsed from punctual.toml (DESIGN O1)."""

    name: str
    schedule: str  # cron expression
    command: list[str]  # argv; shell wrapping is explicit
    timezone: str = "UTC"  # IANA name - DESIGN O1 sub-decision
    missed: MissedPolicy = MissedPolicy.RUN_LATEST
    retries: RetryPolicy = field(default_factory=RetryPolicy)
    timeout: timedelta | None = None
    after: list[str] = field(default_factory=list)  # dependency edges (M4)
    idempotent: bool = False  # safe to re-run; drives process group + on_lost (O2b)
    on_lost: OnLost | None = None  # None -> derive from idempotent (O2b)
    concurrency: int = 1  # max simultaneous runs of THIS job
    quarantine_after: int = 5  # consecutive failures -> QUARANTINED
    on_fail: str | None = None  # notification URI
    workdir: str | None = None
    env: dict[str, str] = field(default_factory=dict)
    enabled: bool = True

    @property
    def effective_on_lost(self) -> OnLost:
        """The on_lost policy after applying the idempotent-derived default (O2b)."""
        if self.on_lost is not None:
            return self.on_lost
        return OnLost.RETRY if self.idempotent else OnLost.FAIL


@dataclass(slots=True)
class Run:
    """One execution (or attempted execution) of a job's fire."""

    id: int | None
    job: str
    scheduled_for: datetime  # the fire time this run belongs to
    state: RunState = RunState.PENDING
    attempt: int = 1  # 1 = first try
    claimed_by: str | None = None  # daemon instance id (fencing later)
    started_at: datetime | None = None
    finished_at: datetime | None = None
    exit_code: int | None = None
    heartbeat_at: datetime | None = None  # LOST detection (DESIGN O2)
    pid: int | None = None  # child pid while RUNNING (O2b recovery)
    pid_start_time: str | None = None  # pid identity check on restart (O2b)

    def transition_to(self, new: RunState) -> None:
        allowed = _TRANSITIONS.get(self.state, set())
        if new not in allowed:
            raise InvalidTransition(f"{self.job} run {self.id}: {self.state} -> {new}")
        self.state = new
