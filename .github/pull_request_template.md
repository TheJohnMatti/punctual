<!--
Title must be a Conventional Commit, e.g. `feat(scheduler): catch-up on restart`
(enforced by the `conventional-title` check). It becomes the squash-merge subject.
-->

## What

<!-- One or two sentences. What does this change and why. -->

## Design

<!-- Link the DESIGN.md entry (D#/O#) this implements or changes. If this makes a
     new non-obvious choice, add an entry in the same PR. -->

## Checklist

- [ ] `docs/DESIGN.md` updated (or N/A — no design decision here)
- [ ] Tests cover the new behaviour
- [ ] `ruff check` / `ruff format` / `mypy` / `pytest` pass locally
- [ ] No new runtime dependency (or: justified below, per DESIGN D7)
