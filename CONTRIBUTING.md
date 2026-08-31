# Contributing

## Dev setup

The environment is [uv](https://docs.astral.sh/uv/)-managed.

```console
uv pip install -e ".[dev]"
uv run pre-commit install    # ruff + hygiene hooks on every commit
```

## Checks (all run in CI)

| Command | What |
|---|---|
| `ruff check .` | lint |
| `ruff format --check .` | formatting |
| `mypy` | strict type-checking (`src/` only) |
| `pytest -q` | tests, on 3.12 and 3.13 |

`pre-commit run --all-files` covers the first two plus file hygiene.

## Pull requests

- PR titles must be [Conventional Commits](https://www.conventionalcommits.org/)
  (`feat:`, `fix:`, `chore:`, `docs:`, `refactor:`, `test:`, `perf:`, `ci:`,
  `build:`) — enforced by the `pr-title` check. The title becomes the
  squash-merge subject.
- Every non-obvious design choice gets an entry in [`docs/DESIGN.md`](docs/DESIGN.md).
- Keep the runtime dependency set minimal (currently `click` + `croniter` — see
  DESIGN D7).

## Releasing

Fully tag-driven — `hatch-vcs` derives the version from the tag, so there is no
version to bump anywhere.

```console
git tag v0.1.0        # -> package version 0.1.0
git push origin v0.1.0
```

The `release` workflow builds the sdist + wheel, publishes to PyPI via OIDC
trusted publishing (no API token), and opens a GitHub Release. Between tags,
local builds get a dev version like `0.1.dev7+g1a2b3c4`.

One-time PyPI setup (not yet done): add a pending publisher at
<https://pypi.org/manage/account/publishing/> — project `punctual-scheduler`,
owner `TheJohnMatti`, repo `punctual`, workflow `release.yml`, environment
`pypi`.
