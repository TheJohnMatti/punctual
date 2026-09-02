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


class UpstreamFailure(enum.StrEnum):
    """What a triggered job (DESIGN O12) does when an upstream fire fails."""

    SKIP = "skip"  # don't run; record a SKIPPED row with the reason
    RUN = "run"  # run anyway — `after` is advisory ordering only
    WAIT = "wait"  # hold until the upstream eventually succeeds


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


# M2.3: a run in one of these outcomes is retried (up to RetryPolicy.max) — a
# timed-out job is usually a transiently slow dependency, so it retries too.
RETRYABLE_OUTCOMES = frozenset({RunState.FAILED, RunState.TIMED_OUT})

# M2.2a: a fire whose *final* outcome is one of these counts toward quarantine.
FAILURE_OUTCOMES = frozenset({RunState.FAILED, RunState.TIMED_OUT, RunState.LOST})


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
    command: list[str]  # argv; shell wrapping is explicit
    schedule: str | None = None  # cron expr — exactly one of schedule / after (O12)
    after: list[str] = field(default_factory=list)  # upstreams; this job is trigger-driven
    on_upstream_failure: UpstreamFailure = UpstreamFailure.SKIP
    wait_timeout: timedelta | None = None  # WAIT: fall back to skip after this long
    timezone: str = "UTC"  # IANA name - DESIGN O1 sub-decision
    missed: MissedPolicy = MissedPolicy.RUN_LATEST
    retries: RetryPolicy = field(default_factory=RetryPolicy)
    timeout: timedelta | None = None
    idempotent: bool = False  # safe to re-run; drives process group + on_lost (O2b)
    on_lost: OnLost | None = None  # None -> derive from idempotent (O2b)
    catch_up_cap: int = 25  # run_each: max missed fires to replay on restart; 0 = uncapped (O3)
    concurrency: int = 1  # max simultaneous runs of THIS job
    quarantine_after: int = 5  # consecutive failed fires -> QUARANTINED (0 disables)
    quarantine_cooldown: timedelta | None = None  # let one probe fire through after this
    on_fail: str | None = None  # notify URI: a fire exhausted its retries
    on_quarantine: str | None = None  # notify URI: the job was quarantined
    on_recovery: str | None = None  # notify URI: a failing job is healthy again
    workdir: str | None = None
    env: dict[str, str] = field(default_factory=dict)
    enabled: bool = True

    @property
    def effective_on_lost(self) -> OnLost:
        """The on_lost policy after applying the idempotent-derived default (O2b)."""
        if self.on_lost is not None:
            return self.on_lost
        return OnLost.RETRY if self.idempotent else OnLost.FAIL

    @property
    def triggered(self) -> bool:
        """True if this job fires on upstream completion, not a clock (O12)."""
        return bool(self.after)


@dataclass(slots=True)
class ObservabilityConfig:
    """The optional ``[observability]`` table in punctual.toml (M3)."""

    metrics_addr: str = "127.0.0.1"
    metrics_port: int | None = None  # None -> no HTTP listener


@dataclass(slots=True)
class StoreConfig:
    """The optional ``[store]`` table in punctual.toml (M5).

    ``url`` picks the backend: ``sqlite:///abs/path`` (or unset → the XDG default
    file) vs ``postgresql://user:pw@host/db``. Postgres is what makes a
    ``--cluster`` deployment actually shared across hosts.
    """

    url: str | None = None


@dataclass(slots=True)
class Config:
    """Everything parsed from punctual.toml."""

    jobs: list[Job]
    observability: ObservabilityConfig = field(default_factory=ObservabilityConfig)
    store: StoreConfig = field(default_factory=StoreConfig)


@dataclass(slots=True)
class JobState:
    """Per-job state that outlives any single run (store.job_clock row).

    ``consecutive_failures`` counts *fires* (not attempts) whose final outcome
    was a failure; one success resets it. At ``Job.quarantine_after`` the job is
    quarantined: ``quarantined_at`` is set and its fires are skipped (counted in
    ``skipped_quarantined``) until a `punctual resume` or a cooldown probe.
    """

    job: str
    last_fire: datetime | None = None
    consecutive_failures: int = 0
    quarantined_at: datetime | None = None
    quarantine_reason: str | None = None
    skipped_quarantined: int = 0
    resume_requested: bool = False

    @property
    def quarantined(self) -> bool:
        return self.quarantined_at is not None


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
    stdout_tail: str | None = None  # last O5.TAIL_BYTES of stdout, decoded
    stderr_tail: str | None = None  # last O5.TAIL_BYTES of stderr, decoded
    not_before: datetime | None = None  # M2: a RETRYING row is due at/after this
    created_at: datetime | None = None  # when the row was claimed (for `why`)
    note: str | None = None  # free-text: LOST cause, "upstream X failed", etc.

    @property
    def duration(self) -> timedelta | None:
        if self.started_at and self.finished_at:
            return self.finished_at - self.started_at
        return None

    def transition_to(self, new: RunState) -> None:
        allowed = _TRANSITIONS.get(self.state, set())
        if new not in allowed:
            raise InvalidTransition(f"{self.job} run {self.id}: {self.state} -> {new}")
        self.state = new
