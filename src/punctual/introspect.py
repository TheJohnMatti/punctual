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


def _upstream_status(job: Job, store: Store) -> dict[str, Any]:
    """Per-upstream readiness for a triggered job (O12) — the read-only twin of
    the scheduler's `_dispatch_triggered` gate."""
    last = store.job_state(job.name).last_fire
    ups: list[dict[str, Any]] = []
    for up in job.after:
        ok = store.last_success_fire(up)
        lr = store.last_run(up)
        if ok is not None and (last is None or ok > last):
            state = "ready"
        elif lr and lr.state in FAILURE_OUTCOMES and (last is None or lr.scheduled_for > last):
            state = "failed"
        else:
            state = "pending"
        ups.append(
            {
                "job": up,
                "state": state,
                "last_success": _iso(ok),
                "last_run_state": lr.state.value if lr else None,
            }
        )
    if any(u["state"] == "failed" for u in ups):
        overall = "blocked"
    elif ups and all(u["state"] == "ready" for u in ups):
        overall = "ready"
    else:
        overall = "waiting"
    return {"trigger_state": overall, "upstreams": ups}


def explain_job(
    job: Job, store: Store, now: datetime | None = None, all_jobs: list[Job] | None = None
) -> dict[str, Any]:
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

    nxt = None if job.schedule is None else _iso(next_fire(job.schedule, now, job.timezone))

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

    downstreams = (
        sorted(j.name for j in all_jobs if job.name in j.after) if all_jobs is not None else None
    )
    return {
        "job": job.name,
        "schedule": job.schedule,
        "python_ref": job.python_ref,
        "after": job.after or None,
        "downstreams": downstreams or None,
        "depends": _upstream_status(job, store) if job.triggered else None,
        "timezone": job.timezone,
        "enabled": job.enabled,
        "health": health,
        "consecutive_failures": st.consecutive_failures,
        "quarantine": quarantine,
        "last_run": _run_brief(last) if last else None,
        "next_fire": nxt,
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
        "steps": [name for name, _ in store.steps_for(run.job, run.scheduled_for)] or None,
        "stdout_tail": run.stdout_tail,
        "stderr_tail": run.stderr_tail,
    }
