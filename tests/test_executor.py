import sys
from datetime import datetime, timedelta

from punctual.executor import TAIL_BYTES, _Ring, execute, kill_group
from punctual.models import Job, Run, RunState


def _job(*argv: str, timeout: timedelta | None = None) -> Job:
    return Job(name="t", schedule="* * * * *", command=list(argv), timeout=timeout)


def _run() -> Run:
    return Run(id=1, job="t", scheduled_for=datetime(2026, 1, 1), state=RunState.CLAIMED)


def test_ring_keeps_only_the_tail():
    r = _Ring(8)
    r.write(b"12345")
    r.write(b"6789ab")
    assert r.bytes() == b"456789ab"


async def test_success_captures_stdout():
    out = await execute(_job(sys.executable, "-c", "print('hello')"), _run(), timeout=None)
    assert out.exit_code == 0
    assert out.timed_out is False
    assert out.stdout_tail.strip() == b"hello"


async def test_nonzero_exit_is_not_an_error():
    out = await execute(_job(sys.executable, "-c", "import sys; sys.exit(3)"), _run(), timeout=None)
    assert out.exit_code == 3
    assert out.timed_out is False


async def test_stderr_is_captured_separately():
    out = await execute(
        _job(sys.executable, "-c", "import sys; print('boom', file=sys.stderr)"),
        _run(),
        timeout=None,
    )
    assert out.stdout_tail == b""
    assert out.stderr_tail.strip() == b"boom"


async def test_run_id_is_in_the_child_env():
    out = await execute(
        _job(sys.executable, "-c", "import os; print(os.environ['PUNCTUAL_RUN_ID'])"),
        _run(),
        timeout=None,
    )
    assert out.stdout_tail.strip() == b"1"


async def test_timeout_kills_and_flags():
    out = await execute(
        _job(sys.executable, "-c", "import time; time.sleep(30)", timeout=timedelta(seconds=0.3)),
        _run(),
        timeout=timedelta(seconds=0.3),
    )
    assert out.timed_out is True
    assert out.exit_code != 0


async def test_timeout_kills_the_whole_group_not_just_the_shell():
    # bash waits on its `sleep` child; a group-wide kill must still be fast.
    started = datetime.now().timestamp()
    out = await execute(
        _job("bash", "-lc", "sleep 30; echo done", timeout=timedelta(seconds=0.3)),
        _run(),
        timeout=timedelta(seconds=0.3),
    )
    assert out.timed_out is True
    assert datetime.now().timestamp() - started < 5  # not ~30s


async def test_on_spawn_reports_the_pid():
    seen: list[int] = []
    await execute(
        _job(sys.executable, "-c", "pass"),
        _run(),
        timeout=None,
        on_spawn=seen.append,
    )
    assert len(seen) == 1 and seen[0] > 0


def test_kill_group_tolerates_a_dead_pid():
    kill_group(2**30)  # no such process — must not raise


async def test_missing_command_is_exit_127():
    out = await execute(_job("punctual-no-such-binary-xyz"), _run(), timeout=None)
    assert out.exit_code == 127
    assert out.stderr_tail  # carries the reason


async def test_output_tail_is_bounded():
    # print ~200 KiB; we should keep only the last TAIL_BYTES
    code = "print('x' * 1000)\n" * 200
    out = await execute(_job(sys.executable, "-c", code), _run(), timeout=None)
    assert len(out.stdout_tail) <= TAIL_BYTES
