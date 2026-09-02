"""M6 slice 1 — @punctual.job + in-process execution."""

import subprocess
import sys
import textwrap
import uuid

import pytest

from punctual import job, registry
from punctual.config import ConfigError, load_config


def _module(tmp_path, monkeypatch, body: str) -> str:
    name = f"pnc_jobs_{uuid.uuid4().hex[:12]}"
    (tmp_path / f"{name}.py").write_text(textwrap.dedent(body))
    monkeypatch.syspath_prepend(str(tmp_path))
    return name


def _config(tmp_path, module: str, extra: str = "") -> str:
    p = tmp_path / "punctual.toml"
    p.write_text(f'[python]\nmodules = ["{module}"]\n{extra}')
    return str(p)


def test_decorator_registers():
    @job("nightly", schedule="0 3 * * *")
    def _run():
        pass

    assert "nightly" in registry.registered()
    assert _run.__punctual_job__ == "nightly"


def test_decorator_rejects_command():
    with pytest.raises(ValueError, match="synthesised"):
        job("x", command=["true"])


def test_python_job_becomes_a_job(tmp_path, monkeypatch):
    mod = _module(
        tmp_path,
        monkeypatch,
        """
        from punctual import job

        @job("greet", schedule="*/5 * * * *", retries={"max": 2}, timeout="1h")
        def greet():
            print("hi")
        """,
    )
    cfg = load_config(_config(tmp_path, mod))
    (j,) = cfg.jobs
    assert j.name == "greet" and j.schedule == "*/5 * * * *"
    assert j.retries.max == 2 and j.timeout.total_seconds() == 3600
    assert j.python_ref == f"{mod}:greet"
    assert j.command == [sys.executable, "-m", "punctual._inproc", f"{mod}:greet"]


def test_python_and_toml_jobs_coexist(tmp_path, monkeypatch):
    mod = _module(
        tmp_path,
        monkeypatch,
        """
        from punctual import job

        @job("pyjob", schedule="@daily")
        def pyjob():
            pass
        """,
    )
    cfg = load_config(
        _config(tmp_path, mod, extra='\n[job.shjob]\nschedule = "@hourly"\ncommand = "true"\n')
    )
    assert {j.name for j in cfg.jobs} == {"pyjob", "shjob"}


def test_duplicate_name_across_toml_and_decorator_is_rejected(tmp_path, monkeypatch):
    mod = _module(
        tmp_path,
        monkeypatch,
        """
        from punctual import job

        @job("dup", schedule="@daily")
        def dup():
            pass
        """,
    )
    cfg_path = _config(tmp_path, mod, extra='\n[job.dup]\nschedule = "@hourly"\ncommand = "true"\n')
    with pytest.raises(ConfigError, match="defined twice"):
        load_config(cfg_path)


def test_bad_options_surface_as_config_error(tmp_path, monkeypatch):
    mod = _module(
        tmp_path,
        monkeypatch,
        """
        from punctual import job

        @job("both", schedule="@daily", after=["x"])
        def both():
            pass
        """,
    )
    with pytest.raises(ConfigError, match="exactly one of"):
        load_config(_config(tmp_path, mod))


def test_unimportable_module_is_a_config_error(tmp_path):
    p = tmp_path / "punctual.toml"
    p.write_text('[python]\nmodules = ["does_not_exist_pnc"]\n')
    with pytest.raises(ConfigError, match="cannot import"):
        load_config(str(p))


def test_inproc_runs_the_function(tmp_path, monkeypatch):
    mod = _module(
        tmp_path,
        monkeypatch,
        """
        from punctual import job

        @job("w", schedule="@daily")
        def w():
            print("worked")
        """,
    )
    r = subprocess.run(
        [sys.executable, "-m", "punctual._inproc", f"{mod}:w"],
        capture_output=True,
        text=True,
        cwd=tmp_path,
    )
    assert r.returncode == 0 and "worked" in r.stdout


def test_inproc_propagates_failure(tmp_path, monkeypatch):
    mod = _module(
        tmp_path,
        monkeypatch,
        """
        from punctual import job

        @job("boom", schedule="@daily")
        def boom():
            raise RuntimeError("nope")
        """,
    )
    r = subprocess.run(
        [sys.executable, "-m", "punctual._inproc", f"{mod}:boom"],
        capture_output=True,
        text=True,
        cwd=tmp_path,
    )
    assert r.returncode != 0 and "RuntimeError: nope" in r.stderr
