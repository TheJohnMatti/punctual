"""The service files ship as documentation-that-runs; keep them parseable."""

import configparser
import plistlib
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PKG = ROOT / "packaging"


def test_launchd_plist_is_valid_and_calls_run():
    path = PKG / "launchd" / "com.github.thejohnmatti.punctual.plist"
    plist = plistlib.loads(path.read_bytes())
    assert plist["Label"] == "com.github.thejohnmatti.punctual"
    assert plist["ProgramArguments"][-1] == "run"
    assert plist["RunAtLoad"] is True
    assert plist["KeepAlive"] is True


def test_systemd_unit_parses_and_drains_on_stop():
    # interpolation=None: systemd's %h etc. are not configparser interpolation
    cp = configparser.ConfigParser(strict=False, interpolation=None)
    cp.read(PKG / "systemd" / "punctual.service")
    assert cp["Service"]["ExecStart"].endswith("run")
    assert cp["Service"]["KillSignal"] == "SIGINT"  # graceful drain, not SIGKILL
    assert cp["Service"]["Restart"] == "always"
    assert cp["Install"]["WantedBy"] == "default.target"
