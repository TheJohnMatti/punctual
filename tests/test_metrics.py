import asyncio
import urllib.request
from datetime import UTC, datetime, timedelta

from punctual import metrics
from punctual.models import Job, JobState, RunState
from punctual.store import SqliteStore


def _job(name="j", schedule="0 * * * *") -> Job:
    return Job(name=name, schedule=schedule, command=["true"])


_n = 0


def _finished(store, job, state, exit_code, dur_s=2.0):
    global _n
    _n += 1
    fire = datetime.now(UTC).replace(microsecond=0) - timedelta(hours=_n)
    r = store.claim(job, fire, "t")
    r.transition_to(RunState.RUNNING)
    r.started_at = datetime.now(UTC) - timedelta(seconds=dur_s)
    r.finished_at = datetime.now(UTC)
    r.transition_to(state)
    r.exit_code = exit_code
    store.mark(r)
    return r


def test_render_has_the_core_metrics(tmp_path):
    store = SqliteStore(tmp_path / "s.db")
    _finished(store, "j", RunState.SUCCEEDED, 0)
    _finished(store, "j", RunState.FAILED, 1)

    text = metrics.render(store, [_job()], loop_lag=0.01)

    assert "# TYPE punctual_runs_total counter" in text
    assert 'punctual_runs_total{job="j",state="succeeded"} 1' in text
    assert 'punctual_runs_total{job="j",state="failed"} 1' in text
    assert 'punctual_time_since_last_success_seconds{job="j"}' in text
    assert 'punctual_job_quarantined{job="j"} 0' in text
    assert "punctual_run_duration_seconds_bucket" in text
    assert "punctual_scheduler_loop_lag_seconds 0.01" in text
    store.close()


def test_render_reflects_quarantine(tmp_path):
    store = SqliteStore(tmp_path / "s.db")
    store.save_job_state(
        JobState(job="j", consecutive_failures=5, quarantined_at=datetime.now(UTC))
    )
    text = metrics.render(store, [_job()])
    assert 'punctual_job_quarantined{job="j"} 1' in text
    assert 'punctual_job_consecutive_failures{job="j"} 5' in text
    store.close()


async def test_http_server_serves_metrics_and_healthz(tmp_path):
    store = SqliteStore(tmp_path / "s.db")
    _finished(store, "j", RunState.SUCCEEDED, 0)

    async with metrics.http_server(
        "127.0.0.1", 0, lambda: metrics.render(store, [_job()]), lambda: (True, "ok")
    ) as server:
        port = server.sockets[0].getsockname()[1]
        body = await asyncio.to_thread(
            lambda: urllib.request.urlopen(f"http://127.0.0.1:{port}/metrics").read().decode()
        )
        assert "punctual_runs_total" in body
        health = await asyncio.to_thread(
            lambda: urllib.request.urlopen(f"http://127.0.0.1:{port}/healthz").read().decode()
        )
        assert health.strip() == "ok"
    store.close()
