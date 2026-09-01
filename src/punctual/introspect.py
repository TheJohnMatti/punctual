"""Read-only explanations for `punctual why` / `status` / `plan` (M2 slice 3).

Everything here is derived from the config + the SQLite store — no running
daemon required. Functions return plain JSON-able dicts; the CLI renders them
as narrative or with ``--json``.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from punctual.models import FAILURE_OUTCOMES, Job, Run, RunState
from punctual.schedule import next_fire
from punctual.store import Store


def _iso(dt: datetime | None) -> str | None:
    return dt.isoformat(timespec="seconds") if dt else None


def _run_brief(r: Run) -> dict[str, Any]:
    return {
        "run_id": r.id,
        "scheduled_for": _iso(r.scheduled_for),
        "attempt": r.attempt,
        "state": r.state.value,
        "exit_code": r.exit_code,
        "started_at": _iso(r.started_at),
        "finished_at": _iso(r.finished_at),
        "duration_s": round(r.duration.total_seconds(), 2) if r.duration else None,
    }


def explain_job(job: Job, store: Store, now: datetime | None = None) -> dict[str, Any]:
    now = now or datetime.now(UTC)
    st = store.job_state(job.name)
    recent = store.history(job.name, limit=1)
    last = recent[0] if recent else None
    retry = store.pending_retry(job.name)

    if not job.enabled:
        health = "disabled"
    elif st.quarantined:
        health = "quarantined"
    elif st.consecutive_failures:
        health = "degraded"
    else:
        health = "ok"

    quarantine: dict[str, Any] | None = None
    if st.quarantined:
        cooldown_at = (
            st.quarantined_at + job.quarantine_cooldown
            if job.quarantine_cooldown and st.quarantined_at
            else None
        )
        quarantine = {
            "since": _iso(st.quarantined_at),
            "reason": st.quarantine_reason,
            "fires_skipped": st.skipped_quarantined,
            "probe_at": _iso(cooldown_at),
        }

    return {
        "job": job.name,
        "schedule": job.schedule,
        "timezone": job.timezone,
        "enabled": job.enabled,
        "health": health,
        "consecutive_failures": st.consecutive_failures,
        "quarantine": quarantine,
        "last_run": _run_brief(last) if last else None,
        "next_fire": _iso(next_fire(job.schedule, now, job.timezone)),
        "pending_retry": (
            {"attempt": retry.attempt, "not_before": _iso(retry.not_before)} if retry else None
        ),
    }


def _trigger(run: Run) -> str:
    if run.attempt > 1:
        return f"retry of attempt {run.attempt - 1}"
    if run.created_at and (run.created_at - run.scheduled_for).total_seconds() > 90:
        return "catch-up (fire came due while the daemon was down)"
    return "scheduled fire"


def _what_next(run: Run, siblings: list[Run], store: Store) -> str:
    later = [s for s in siblings if s.attempt > run.attempt]
    if run.state is RunState.SUCCEEDED:
        n = sum(1 for s in siblings if s.attempt < run.attempt)
        return f"succeeded after {n} failed attempt(s)" if n else "succeeded"
    if run.state in {RunState.CLAIMED, RunState.RUNNING, RunState.RETRYING}:
        return "still in progress"
    if run.state in FAILURE_OUTCOMES:
        if later:
            nxt = min(later, key=lambda s: s.attempt)
            return f"retried as attempt {nxt.attempt} ({nxt.state.value})"
        st = store.job_state(run.job)
        if st.quarantined:
            return "retries exhausted — job then quarantined"
        return f"retries exhausted — {run.state.value} stands"
    return run.state.value


def explain_run(run: Run, store: Store) -> dict[str, Any]:
    siblings = store.attempts_for(run.job, run.scheduled_for)
    priors = [s for s in siblings if s.attempt < run.attempt]
    return {
        **_run_brief(run),
        "job": run.job,
        "trigger": _trigger(run),
        "prior_attempts": [
            {"attempt": s.attempt, "state": s.state.value, "exit_code": s.exit_code} for s in priors
        ],
        "what_happened_next": _what_next(run, siblings, store),
        "stdout_tail": run.stdout_tail,
        "stderr_tail": run.stderr_tail,
    }
