import json
import logging

from punctual.logs import JSONFormatter


def _record(**extra) -> logging.LogRecord:
    r = logging.LogRecord(
        "punctual.scheduler", logging.INFO, __file__, 1, "hi %s", ("there",), None
    )
    r.__dict__.update(extra)
    return r


def test_json_formatter_surfaces_structured_fields():
    line = JSONFormatter().format(_record(event="run_finished", job="backup", run_id=7))
    obj = json.loads(line)
    assert obj["msg"] == "hi there"
    assert obj["level"] == "info"
    assert obj["logger"] == "punctual.scheduler"
    assert obj["event"] == "run_finished" and obj["job"] == "backup" and obj["run_id"] == 7
    assert obj["ts"].endswith("Z")


def test_json_formatter_drops_internal_logrecord_noise():
    obj = json.loads(JSONFormatter().format(_record()))
    for noise in ("pathname", "processName", "relativeCreated", "args"):
        assert noise not in obj


def test_configure_swaps_the_root_handler(capsys):
    from punctual import logs

    logs.configure("json", verbose=False)
    logging.getLogger("punctual.x").info("hello", extra={"event": "test"})
    err = capsys.readouterr().err
    assert json.loads(err.strip())["event"] == "test"
    logs.configure("text", verbose=False)  # restore
