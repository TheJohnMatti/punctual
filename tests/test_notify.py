import http.server
import json
import sys
import threading

from punctual import notify

EVENT = {"event": "fail", "job": "backup", "reason": "run failed"}


async def test_exec_sink_gets_the_event_on_stdin(tmp_path):
    out = tmp_path / "got.json"
    script = tmp_path / "s.py"
    script.write_text("import sys, pathlib\npathlib.Path(sys.argv[1]).write_text(sys.stdin.read())")
    await notify.send(f"exec:{sys.executable} {script} {out}", EVENT)
    assert json.loads(out.read_text()) == EVENT


async def test_exec_sink_substitutes_placeholders(tmp_path):
    out = tmp_path / "got.txt"
    script = tmp_path / "s.py"
    script.write_text("import sys, pathlib\npathlib.Path(sys.argv[1]).write_text(sys.argv[2])")
    await notify.send(f"exec:{sys.executable} {script} {out} {{job}}", EVENT)
    assert out.read_text() == "backup"


async def test_webhook_sink_posts_json(tmp_path):
    received: list[dict] = []

    class H(http.server.BaseHTTPRequestHandler):
        def do_POST(self):
            n = int(self.headers["Content-Length"])
            received.append(json.loads(self.rfile.read(n)))
            self.send_response(200)
            self.end_headers()

        def log_message(self, *_):  # keep the test quiet
            pass

    srv = http.server.HTTPServer(("127.0.0.1", 0), H)
    threading.Thread(target=srv.handle_request, daemon=True).start()
    await notify.send(f"http://127.0.0.1:{srv.server_port}/hook", EVENT)
    srv.server_close()
    assert received == [EVENT]


async def test_a_broken_sink_does_not_raise():
    await notify.send("exec:/no/such/program", EVENT)  # logs, returns normally
    await notify.send("ftp://nope", EVENT)  # unknown scheme


def test_check_flags_unknown_schemes():
    problems = notify.check(["ntfy://alerts", "slakc://typo", None, "exec:echo hi"])
    assert list(problems) == ["slakc://typo"]


async def test_ntfy_slack_discord_hit_the_right_url(monkeypatch):
    seen: list = []

    def fake_urlopen(req, timeout=None):
        seen.append((req.full_url, req.data, dict(req.headers)))

        class R:
            status = 200

            def __enter__(self):
                return self

            def __exit__(self, *_):
                return False

        return R()

    monkeypatch.setattr("urllib.request.urlopen", fake_urlopen)

    await notify.send("ntfy://mytopic", EVENT)
    await notify.send("slack://T1/B2/tok", EVENT)
    await notify.send("discord://111/tok", EVENT)

    urls = [s[0] for s in seen]
    assert urls == [
        "https://ntfy.sh/mytopic",
        "https://hooks.slack.com/services/T1/B2/tok",
        "https://discord.com/api/webhooks/111/tok",
    ]
    assert seen[0][2].get("Priority") == "high"  # a "fail" event
    assert json.loads(seen[1][1])["text"].startswith("punctual: backup")


async def test_entry_point_plugin_is_discovered(monkeypatch):
    hits: list[str] = []

    async def my_sink(uri, event):
        hits.append(uri)

    class _EP:
        name = "custom"

        def load(self):
            return my_sink

    monkeypatch.setattr(notify, "entry_points", lambda group: [_EP()])
    notify.load_sinks()
    try:
        assert "custom" in notify._registry
        await notify.send("custom://whatever", EVENT)
        assert hits == ["custom://whatever"]
    finally:
        notify.load_sinks()  # restore the real registry
