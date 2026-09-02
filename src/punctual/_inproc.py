"""``python -m punctual._inproc pkg.module:function`` — run one ``@punctual.job``.

The scheduler synthesises this as a job's ``command`` (wrapped, like every job,
by :mod:`punctual._runner`). Importing the module runs the ``@punctual.job``
decorator; we then call the function. An exception propagates → non-zero exit +
traceback on stderr, which the executor captures and the run lands FAILED.
"""

from __future__ import annotations

import importlib
import sys


def main(argv: list[str]) -> int:
    if len(argv) != 1 or ":" not in argv[0]:
        print("usage: python -m punctual._inproc <module>:<function>", file=sys.stderr)
        return 2
    module_name, _, attr = argv[0].partition(":")

    # so a job module resolves when the daemon runs from the project directory
    if "" not in sys.path:
        sys.path.insert(0, "")

    module = importlib.import_module(module_name)
    fn = getattr(module, attr, None)
    if fn is None or not callable(fn):
        print(f"{argv[0]}: no callable {attr!r} in {module_name!r}", file=sys.stderr)
        return 2

    fn()
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
