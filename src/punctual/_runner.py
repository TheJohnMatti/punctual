"""Exit-code sentinel wrapper (DESIGN O2b).

``python -m punctual._runner <run_dir> -- <argv...>``

Runs the real command as a child, and on exit writes ``<run_dir>/exit`` with the
child's outcome *before* returning that same code. If the daemon dies while the
job is running, recovery reads this file and resolves the run to its true
SUCCEEDED / FAILED / TIMED_OUT instead of a blind LOST.

Costs one lightweight process per run and needs no cooperation from the job.
SIGTERM / SIGINT are forwarded to the child and then absorbed here, so the
sentinel still gets written when a timeout or ``stop --kill`` takes the group.
"""

from __future__ import annotations

import contextlib
import json
import os
import signal
import subprocess
import sys
import tempfile
from datetime import UTC, datetime
from pathlib import Path


def _write_sentinel(run_dir: Path, returncode: int) -> None:
    payload = {
        "code": returncode,  # subprocess convention: negative => killed by -N
        "signaled": returncode < 0,
        "at": datetime.now(UTC).isoformat(timespec="seconds"),
    }
    fd, tmp = tempfile.mkstemp(dir=run_dir, prefix=".exit.")
    with os.fdopen(fd, "w") as f:
        json.dump(payload, f)
    os.replace(tmp, run_dir / "exit")  # atomic


def main(argv: list[str]) -> int:
    run_dir = Path(argv[0])
    sep = argv.index("--")
    command = argv[sep + 1 :]

    child = subprocess.Popen(command)  # argv comes from punctual.toml, already split

    def _forward(signum: int, _frame: object) -> None:
        with contextlib.suppress(ProcessLookupError):
            child.send_signal(signum)

    for sig in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP):
        signal.signal(sig, _forward)

    while True:
        try:
            rc = child.wait()
            break
        except KeyboardInterrupt:  # SIGINT already forwarded; keep waiting
            continue

    with contextlib.suppress(OSError):  # a missing run_dir must not mask the exit code
        _write_sentinel(run_dir, rc)

    return 128 + (-rc) if rc < 0 else rc


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
