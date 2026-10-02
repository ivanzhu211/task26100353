"""Background worker: claims dirty learners and recomputes profiles."""

from __future__ import annotations

import os
import random
import sqlite3
import time
import uuid

from .db import connect, transaction
from .recompute import compute_profile

DEFAULT_LEASE_SECONDS = 30.0
DEFAULT_MAX_ATTEMPTS = 5
DEFAULT_BACKOFF_BASE = 1.0


class Worker:
    def __init__(
        self,
        db_path: str,
        worker_id: str | None = None,
        lease_seconds: float = DEFAULT_LEASE_SECONDS,
        max_attempts: int = DEFAULT_MAX_ATTEMPTS,
        backoff_base: float = DEFAULT_BACKOFF_BASE,
        batch_size: int = 16,
        poll_interval: float = 0.2,
    ) -> None:
        self.db_path = str(db_path)
        self.worker_id = worker_id or f"{os.getpid()}-{uuid.uuid4().hex[:8]}"
        self.lease_seconds = lease_seconds
        self.max_attempts = max_attempts
        self.backoff_base = backoff_base
        self.batch_size = batch_size
        self.poll_interval = poll_interval

    # -- claiming ---------------------------------------------------------

    def claim_batch(self, db: sqlite3.Connection) -> list[tuple[str, str]]:
        """Claim due learners, taking over expired leases. Returns (id, token)."""
        now = time.time()
        claimed: list[tuple[str, str]] = []
        with transaction(db):
            rows = db.execute(
                """
                SELECT learner_id, status, attempts FROM recalc_queue
                WHERE (status = 'pending' AND next_retry_at <= ?)
                   OR (status = 'running' AND lease_expires < ?)
                ORDER BY updated_at
                LIMIT ?
                """,
                (now, now, self.batch_size),
            ).fetchall()
            for learner_id, status, attempts in rows:
                if status == "running":
                    # Previous owner was killed or stuck: that run failed.
                    attempts += 1
                    if attempts >= self.max_attempts:
                        self._to_dead_letter(
                            db, learner_id, "previous worker lost (lease expired)"
                        )
                        continue
                token = uuid.uuid4().hex
                db.execute(
                    """
                    UPDATE recalc_queue
                    SET status = 'running', attempts = ?, lease_owner = ?,
                        lease_token = ?, lease_expires = ?, updated_at = ?
                    WHERE learner_id = ?
                    """,
                    (
                        attempts,
                        self.worker_id,
                        token,
                        now + self.lease_seconds,
                        now,
                        learner_id,
                    ),
                )
                claimed.append((learner_id, token))
        return claimed

    # -- bookkeeping ------------------------------------------------------

    def _to_dead_letter(
        self, db: sqlite3.Connection, learner_id: str, reason: str
    ) -> None:
        db.execute(
            """
            INSERT OR REPLACE INTO dead_letters (learner_id, reason, failed_at)
            VALUES (?, ?, ?)
            """,
            (learner_id, reason, time.time()),
        )
        db.execute("DELETE FROM recalc_queue WHERE learner_id = ?", (learner_id,))

    def _complete(
        self,
        db: sqlite3.Connection,
        learner_id: str,
        token: str,
        profile: list,
    ) -> bool:
        """Write the profile back; discarded if the lease was lost."""
        now = time.time()
        with transaction(db):
            row = db.execute(
                "SELECT needs_rerun FROM recalc_queue "
                "WHERE learner_id = ? AND lease_token = ?",
                (learner_id, token),
            ).fetchone()
            if row is None:
                return False
            db.execute("DELETE FROM profiles WHERE learner_id = ?", (learner_id,))
            db.executemany(
                "INSERT INTO profiles (learner_id, tag, mastered_count, "
                "max_difficulty) VALUES (?, ?, ?, ?)",
                [
                    (learner_id, e.tag, e.mastered_count, int(e.max_difficulty))
                    for e in profile
                ],
            )
            if row[0]:
                db.execute(
                    """
                    UPDATE recalc_queue
                    SET status = 'pending', needs_rerun = 0, attempts = 0,
                        next_retry_at = 0, lease_owner = NULL, lease_token = NULL,
                        lease_expires = NULL, updated_at = ?
                    WHERE learner_id = ?
                    """,
                    (now, learner_id),
                )
            else:
                db.execute(
                    "DELETE FROM recalc_queue WHERE learner_id = ?", (learner_id,)
                )
        return True

    def _fail(
        self, db: sqlite3.Connection, learner_id: str, token: str, reason: str
    ) -> None:
        now = time.time()
        with transaction(db):
            row = db.execute(
                "SELECT attempts FROM recalc_queue "
                "WHERE learner_id = ? AND lease_token = ?",
                (learner_id, token),
            ).fetchone()
            if row is None:
                return
            attempts = row[0] + 1
            if attempts >= self.max_attempts:
                self._to_dead_letter(db, learner_id, reason)
            else:
                delay = self.backoff_base * (2 ** (attempts - 1))
                db.execute(
                    """
                    UPDATE recalc_queue
                    SET status = 'pending', attempts = ?, next_retry_at = ?,
                        lease_owner = NULL, lease_token = NULL,
                        lease_expires = NULL, updated_at = ?
                    WHERE learner_id = ?
                    """,
                    (attempts, now + delay, now, learner_id),
                )

    # -- recomputation ----------------------------------------------------

    def recompute(self, db: sqlite3.Connection, learner_id: str) -> list:
        self._maybe_inject_fault(learner_id)
        return compute_profile(db, learner_id)

    @staticmethod
    def _maybe_inject_fault(learner_id: str) -> None:
        """Test hooks via environment variables."""
        if learner_id in os.environ.get("SKILLRECALC_HANG_LEARNERS", "").split(","):
            time.sleep(float(os.environ.get("SKILLRECALC_HANG_SECONDS", "60")))
        if learner_id in os.environ.get("SKILLRECALC_FAIL_LEARNERS", "").split(","):
            raise RuntimeError(f"injected failure for {learner_id}")
        rate = float(os.environ.get("SKILLRECALC_FAIL_RATE", "0"))
        if rate and random.random() < rate:
            raise RuntimeError(f"injected random failure for {learner_id}")
        delay = float(os.environ.get("SKILLRECALC_RECOMPUTE_DELAY", "0"))
        if delay:
            time.sleep(delay)

    @staticmethod
    def _maybe_hang_before_write(learner_id: str) -> None:
        """Test hook: get stuck after computing but before writing back."""
        ids = os.environ.get("SKILLRECALC_HANG_BEFORE_WRITE_LEARNERS", "")
        if learner_id in ids.split(","):
            time.sleep(
                float(os.environ.get("SKILLRECALC_HANG_BEFORE_WRITE_SECONDS", "60"))
            )

    # -- main loop ----------------------------------------------------------

    def run_once(self, db: sqlite3.Connection) -> int:
        claimed = self.claim_batch(db)
        for learner_id, token in claimed:
            try:
                profile = self.recompute(db, learner_id)
            except Exception as exc:  # noqa: BLE001 - any failure counts
                self._fail(db, learner_id, token, f"{type(exc).__name__}: {exc}")
            else:
                self._maybe_hang_before_write(learner_id)
                self._complete(db, learner_id, token, profile)
        return len(claimed)

    def run_forever(self, stop_after_empty: bool = False) -> None:
        db = connect(self.db_path)
        try:
            while True:
                processed = self.run_once(db)
                if processed == 0:
                    if stop_after_empty:
                        row = db.execute(
                            "SELECT 1 FROM recalc_queue LIMIT 1"
                        ).fetchone()
                        if row is None:
                            return
                    time.sleep(self.poll_interval)
        finally:
            db.close()
