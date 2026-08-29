"""The daemon (DESIGN D2). Owns the asyncio loop, decides what fires when,
claims each fire exactly once, hands it to the executor, records the result.

STUB — the reconcile/tick loop lands in M1, catch-up in M2.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass

from punctual.models import Job
from punctual.store import Store


@dataclass(slots=True)
class Scheduler:
    jobs: list[Job]
    store: Store
    instance_id: str  # this daemon's identity (fencing token later)

    async def run(self) -> None:
        """Main loop.

        M1:
          - on start: recover open runs (DESIGN O2 LOST detection), then
            run catch-up per job (DESIGN O3) using store.last_fire()
          - loop: compute the earliest next_fire across all jobs, sleep until it
            (interruptibly), claim it, spawn via executor, record
          - respect job.concurrency and a global cap (DESIGN O_backpressure)
        M2: retries + backoff + quarantine, timeouts, `plan`/`why` introspection
        """
        raise NotImplementedError("Scheduler.run lands in M1")

    async def _recover(self) -> None:
        """Runs left RUNNING/CLAIMED by a dead daemon -> LOST, then re-decide."""
        raise NotImplementedError

    async def _catch_up(self, job: Job) -> None:
        """Apply job.missed to fires that came due while we were down."""
        raise NotImplementedError


async def serve(jobs: list[Job], store: Store, instance_id: str) -> None:
    sched = Scheduler(jobs=jobs, store=store, instance_id=instance_id)
    try:
        await sched.run()
    except asyncio.CancelledError:
        # graceful shutdown: stop claiming, let in-flight runs finish or detach
        raise
