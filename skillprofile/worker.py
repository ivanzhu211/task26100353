"""Background worker loop: claim, recompute, commit."""
from __future__ import annotations

import os
import socket
import sqlite3
import time
from collections.abc import Callable

from . import db, queue, recompute
from .types import TagStat

ComputeFn = Callable[[sqlite3.Connection, str], list[TagStat]]


def run_worker(
    db_path: str,
    worker_id: str | None = None,
    *,
    once: bool = False,
    poll_interval: float = 0.2,
    lease_seconds: float = 30.0,
    base_backoff: float = queue.BASE_BACKOFF_SECONDS,
    max_backoff: float = queue.MAX_BACKOFF_SECONDS,
    max_attempts: int = queue.MAX_ATTEMPTS,
    compute_fn: ComputeFn | None = None,
) -> None:
    """Run the recompute loop until interrupted.

    With once=True, drain all currently claimable jobs and return.
    compute_fn overrides the recompute step (used by tests).
    """
    worker_id = worker_id or f"{socket.gethostname()}:{os.getpid()}"
    compute = compute_fn or recompute.compute_profile
    conn = db.connect(db_path)
    try:
        while True:
            claimed = queue.claim(
                conn,
                worker_id,
                time.time(),
                lease_seconds,
                base_backoff=base_backoff,
                max_backoff=max_backoff,
                max_attempts=max_attempts,
            )
            if claimed is None:
                if once:
                    return
                time.sleep(poll_interval)
                continue
            try:
                profile = compute(conn, claimed.learner_id)
            except Exception as exc:
                queue.fail(
                    conn,
                    claimed,
                    f"{type(exc).__name__}: {exc}",
                    time.time(),
                    base_backoff=base_backoff,
                    max_backoff=max_backoff,
                    max_attempts=max_attempts,
                )
            else:
                queue.commit_success(conn, claimed, profile, time.time())
    finally:
        conn.close()
