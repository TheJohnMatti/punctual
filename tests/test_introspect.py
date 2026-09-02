from datetime import UTC, datetime, timedelta

from punctual.introspect import explain_job, explain_run
from punctual.models import Job, JobState, RunState
from punctual.store import SqliteStore


def _job(name="j", **kw) -> Job:
    return Job(name=name, schedule="0 * * * *", command=["true"], **kw)


def _fire(**kw) -> datetime:
    return datetime.now(UTC).replace(microsecond=0, **kw)


def test_explain_job_healthy(tmp_path):
    store = SqliteStore(tmp_path / "s.db")
    r = explain_job(_job(), store)
    assert r["health"] == "ok"
    assert r["last_run"] is None
    assert r["quarantine"] is None
    assert r["next_fire"] is not None


def test_explain_job_quarantined(tmp_path):
    store = SqliteStore(tmp_path / "s.db")
    store.save_job_state(
        JobState(
            job="j",
            consecutive_failures=5,
            quarantined_at=datetime.now(UTC),
            quarantine_reason="5 consecutive failed fires",
            skipped_quarantined=9,
        )
    )
    r = explain_job(_job(quarantine_cooldown=timedelta(hours=1)), store)
    assert r["health"] == "quarantined"
    assert r["quarantine"]["fires_skipped"] == 9
    assert r["quarantine"]["probe_at"] is not None


def test_explain_run_tells_the_retry_story(tmp_path):
    store = SqliteStore(tmp_path / "s.db")
    fire = _fire()

    a1 = store.claim("j", fire, "d")
    a1.transition_to(RunState.RUNNING)
    a1.transition_to(RunState.FAILED)
    a1.exit_code = 1
    a1.stderr_tail = "boom"
    store.mark(a1)

    a2 = store.schedule_retry("j", fire, 2, datetime.now(UTC), "d")
    a2.transition_to(RunState.CLAIMED)
    a2.transition_to(RunState.RUNNING)
    a2.transition_to(RunState.SUCCEEDED)
    a2.exit_code = 0
    store.mark(a2)

    r1 = explain_run(a1, store)
    assert r1["trigger"] == "scheduled fire"
    assert r1["what_happened_next"] == "retried as attempt 2 (succeeded)"
    assert r1["stderr_tail"] == "boom"

    r2 = explain_run(a2, store)
    assert r2["trigger"] == "retry of attempt 1"
    assert r2["prior_attempts"] == [{"attempt": 1, "state": "failed", "exit_code": 1}]
    assert r2["what_happened_next"] == "succeeded after 1 failed attempt(s)"
    assert r2["steps"] is None
    store.close()


def test_explain_run_lists_completed_steps(tmp_path):
    store = SqliteStore(tmp_path / "s.db")
    fire = _fire()
    run = store.claim("j", fire, "d")
    run.transition_to(RunState.RUNNING)
    run.transition_to(RunState.SUCCEEDED)
    store.mark(run)
    store.record_step("j", fire, "fetch", "[]")
    store.record_step("j", fire, "load", "1")

    assert explain_run(run, store)["steps"] == ["fetch", "load"]
    store.close()


def test_explain_job_dependency_state(tmp_path):
    store = SqliteStore(tmp_path / "s.db")
    up = Job(name="scrape", schedule="0 * * * *", command=["true"])
    down = Job(name="proc", schedule=None, command=["true"], after=["scrape"])

    r = explain_job(down, store, all_jobs=[up, down])
    assert r["depends"]["trigger_state"] == "waiting"  # no scrape success yet
    assert r["depends"]["upstreams"][0]["state"] == "pending"
    assert explain_job(up, store, all_jobs=[up, down])["downstreams"] == ["proc"]

    # scrape succeeds -> proc is ready
    s = store.claim("scrape", _fire(), "t")
    s.transition_to(RunState.RUNNING)
    s.transition_to(RunState.SUCCEEDED)
    store.mark(s)
    r = explain_job(down, store, all_jobs=[up, down])
    assert r["depends"]["trigger_state"] == "ready"
    store.close()
