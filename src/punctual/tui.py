"""`punctual tui` — a read-only Textual dashboard (M3 slice 3).

Polls the SQLite store + config every second (the same read path as
`punctual why` / `status`). No control-socket dependency: it shows last-known
state even when the daemon is down. Requires the ``tui`` extra
(``pip install punctual-scheduler[tui]``).
"""

from __future__ import annotations

import contextlib
from pathlib import Path
from typing import ClassVar

from textual.app import App, ComposeResult
from textual.binding import BindingType
from textual.containers import Horizontal
from textual.widgets import DataTable, Footer, Header, Static

from punctual import introspect
from punctual.config import load_config
from punctual.models import Job
from punctual.store import SqliteStore

_HEALTH_STYLE = {
    "ok": "green",
    "degraded": "yellow",
    "quarantined": "red",
    "disabled": "grey50",
}
_RUN_STYLE = {
    "succeeded": "green",
    "failed": "red",
    "timed_out": "yellow",
    "running": "cyan",
    "lost": "magenta",
    "retrying": "blue",
}


def _short(dt_iso: str | None) -> str:
    return dt_iso.replace("T", " ")[5:16] if dt_iso else "—"


class PunctualTUI(App[None]):
    CSS = """
    #jobs { height: 45%; }
    #detail { width: 45%; padding: 0 1; }
    #runs { width: 55%; }
    """
    BINDINGS: ClassVar[list[BindingType]] = [
        ("q", "quit", "quit"),
        ("r", "refresh", "refresh now"),
    ]

    def __init__(self, config_path: Path) -> None:
        super().__init__()
        self._config_path = config_path
        self._jobs: list[Job] = []
        self._busy = False  # reentrancy guard — table mutations emit RowHighlighted
        self.detail_text = ""  # last rendered detail panel — handy for tests

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        yield DataTable(id="jobs", cursor_type="row")
        with Horizontal():
            yield Static(id="detail")
            yield DataTable(id="runs", cursor_type="row")
        yield Footer()

    def on_mount(self) -> None:
        jt = self.query_one("#jobs", DataTable)
        jt.add_columns("job", "health", "last run", "next fire")
        self.query_one("#runs", DataTable).add_columns("fire", "attempt", "state", "dur", "exit")
        self.set_interval(1.0, self.refresh_data)
        self.refresh_data()
        jt.focus()

    def action_refresh(self) -> None:
        self.refresh_data()

    def on_data_table_row_highlighted(self, event: DataTable.RowHighlighted) -> None:
        # user picked a different job -> re-render its detail. Ignore the echoes
        # from our own add_row / clear on either table during a refresh.
        if self._busy or event.data_table.id != "jobs":
            return
        with contextlib.suppress(Exception):
            store = SqliteStore()
            try:
                self._fill_detail(store)
            finally:
                store.close()

    # --- data ---------------------------------------------------------
    def refresh_data(self) -> None:
        try:
            self._jobs = load_config(self._config_path).jobs
        except Exception as e:  # keep the last good view, note the problem
            self.detail_text = f"config error: {e}"
            self.query_one("#detail", Static).update(f"[red]{self.detail_text}[/red]")
            return
        store = SqliteStore()
        self._busy = True
        try:
            self._fill_jobs(store)
            self._fill_detail(store)
        finally:
            store.close()
            # clear the guard only after Textual drains the RowHighlighted
            # messages our add_row calls just queued
            self.call_after_refresh(self._clear_busy)

    def _clear_busy(self) -> None:
        self._busy = False

    def _selected_job(self) -> Job | None:
        jt = self.query_one("#jobs", DataTable)
        ordered = sorted(self._jobs, key=lambda j: j.name)
        if ordered and 0 <= jt.cursor_row < len(ordered):
            return ordered[jt.cursor_row]
        return ordered[0] if ordered else None

    def _fill_jobs(self, store: SqliteStore) -> None:
        jt = self.query_one("#jobs", DataTable)
        keep = jt.cursor_row
        jt.clear()
        for j in sorted(self._jobs, key=lambda x: x.name):
            r = introspect.explain_job(j, store)
            lr = r["last_run"]
            last = f"{lr['state']} @ {_short(lr['scheduled_for'])}" if lr else "—"
            health = f"[{_HEALTH_STYLE.get(r['health'], 'white')}]{r['health']}[/]"
            jt.add_row(j.name, health, last, _short(r["next_fire"]))
        if jt.row_count:
            jt.move_cursor(row=min(keep, jt.row_count - 1))

    def _fill_detail(self, store: SqliteStore) -> None:
        job = self._selected_job()
        detail = self.query_one("#detail", Static)
        runs_tbl = self.query_one("#runs", DataTable)
        runs_tbl_row = runs_tbl.cursor_row
        was_busy, self._busy = self._busy, True
        runs_tbl.clear()
        if job is None:
            self.detail_text = "no jobs"
            detail.update(self.detail_text)
            self._busy = was_busy
            return

        r = introspect.explain_job(job, store, all_jobs=self._jobs)
        trig = (
            f"after {', '.join(r['after'])}" if r["after"] else f"{r['schedule']}, {r['timezone']}"
        )
        lines = [
            f"[b]{job.name}[/b]  ({trig})",
            f"health: [{_HEALTH_STYLE.get(r['health'], 'white')}]{r['health']}[/]",
        ]
        if r["quarantine"]:
            q = r["quarantine"]
            lines.append(f"[red]quarantined[/red] since {_short(q['since'])}")
            lines.append(f"  {q['reason']}")
            lines.append(f"  {q['fires_skipped']} fires skipped — `punctual resume {job.name}`")
        if r["pending_retry"]:
            pr = r["pending_retry"]
            lines.append(f"retry: attempt {pr['attempt']} at {_short(pr['not_before'])}")
        if r["depends"]:
            d = r["depends"]
            col = {"ready": "green", "waiting": "yellow", "blocked": "red"}[d["trigger_state"]]
            lines.append(f"trigger: [{col}]{d['trigger_state']}[/]")
            mark = {"ready": "✓", "pending": "…", "failed": "✗"}
            lines += [f"  {mark[u['state']]} {u['job']}" for u in d["upstreams"]]
        elif r["next_fire"]:
            lines.append(f"next fire: {_short(r['next_fire'])}")
        if r["downstreams"]:
            lines.append(f"feeds: {', '.join(r['downstreams'])}")

        history = store.history(job.name, limit=20)
        for run in history:
            dur = f"{run.duration.total_seconds():.1f}s" if run.duration else "—"
            code = "—" if run.exit_code is None else str(run.exit_code)
            state = f"[{_RUN_STYLE.get(run.state.value, 'white')}]{run.state.value}[/]"
            runs_tbl.add_row(
                _short(run.scheduled_for.isoformat()),
                str(run.attempt),
                state,
                dur,
                code,
            )
        if runs_tbl.row_count:
            runs_tbl.move_cursor(row=min(runs_tbl_row, runs_tbl.row_count - 1))
        sel = history[runs_tbl.cursor_row] if 0 <= runs_tbl.cursor_row < len(history) else None
        if sel and (sel.stdout_tail or sel.stderr_tail):
            lines.append("")
            lines.append(f"[dim]— output of run {sel.id} —[/dim]")
            for stream, text in (("out", sel.stdout_tail), ("err", sel.stderr_tail)):
                if text:
                    tail = text if len(text) <= 800 else text[-800:]
                    lines += [f"[dim]{stream}:[/dim] {ln}" for ln in tail.splitlines()[-12:]]
        self.detail_text = "\n".join(lines)
        detail.update(self.detail_text)
        self._busy = was_busy


def run_tui(config_path: Path) -> None:
    PunctualTUI(config_path).run()
