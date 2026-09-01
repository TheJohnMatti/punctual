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
