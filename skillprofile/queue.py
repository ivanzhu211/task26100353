"""SQLite-backed recompute job queue with leases, backoff, dead letters."""
from __future__ import annotations

import sqlite3
from dataclasses import dataclass

from .types import DeadLetter, TagStat

MAX_ATTEMPTS = 5
BASE_BACKOFF_SECONDS = 1.0
MAX_BACKOFF_SECONDS = 60.0


@dataclass(frozen=True)
class Claim:
    learner_id: str
    fence: int


def enqueue(conn: sqlite3.Connection, learner_id: str, now: float) -> None:
    """Mark a learner as needing recompute (deduplicated)."""
    conn.execute(
        """
        INSERT INTO jobs (learner_id, state, not_before, updated_at)
        VALUES (?, 'pending', 0, ?)
        ON CONFLICT(learner_id) DO UPDATE SET
            dirty = CASE WHEN jobs.state = 'running' THEN 1 ELSE jobs.dirty END,
            updated_at = excluded.updated_at
        """,
        (learner_id, now),
    )


def _backoff(attempts: int, base: float, cap: float) -> float:
    return min(cap, base * (2.0 ** max(0, attempts - 1)))


def recover_expired_leases(
    conn: sqlite3.Connection,
    now: float,
    *,
    base_backoff: float = BASE_BACKOFF_SECONDS,
    max_backoff: float = MAX_BACKOFF_SECONDS,
    max_attempts: int = MAX_ATTEMPTS,
) -> None:
    """Turn abandoned running jobs into failures (backoff or dead letter)."""
    rows = conn.execute(
        "SELECT learner_id, attempts FROM jobs WHERE state = 'running' AND lease_expires < ?",
        (now,),
    ).fetchall()
    for learner_id, attempts in rows:
        new_attempts = attempts + 1
        if new_attempts >= max_attempts:
            conn.execute(
                """
                UPDATE jobs SET state = 'dead', attempts = ?, dirty = 0,
                    lease_owner = NULL, lease_expires = NULL,
                    last_error = ?, updated_at = ?
                WHERE learner_id = ? AND state = 'running'
                """,
                (new_attempts, "worker lost: lease expired", now, learner_id),
            )
        else:
            conn.execute(
                """
                UPDATE jobs SET state = 'pending', attempts = ?, dirty = 0,
                    lease_owner = NULL, lease_expires = NULL, not_before = ?,
                    last_error = ?, updated_at = ?
                WHERE learner_id = ? AND state = 'running'
                """,
                (
                    new_attempts,
                    now + _backoff(new_attempts, base_backoff, max_backoff),
                    "worker lost: lease expired",
                    now,
                    learner_id,
                ),
            )


def claim(
    conn: sqlite3.Connection,
    worker_id: str,
    now: float,
    lease_seconds: float,
    *,
    base_backoff: float = BASE_BACKOFF_SECONDS,
    max_backoff: float = MAX_BACKOFF_SECONDS,
    max_attempts: int = MAX_ATTEMPTS,
) -> Claim | None:
    """Atomically claim the next due learner; None if nothing is due."""
    recover_expired_leases(
        conn, now, base_backoff=base_backoff, max_backoff=max_backoff,
        max_attempts=max_attempts,
    )
    row = conn.execute(
        """
        SELECT learner_id FROM jobs
        WHERE state = 'pending' AND not_before <= ?
        ORDER BY updated_at, learner_id
        LIMIT 1
        """,
        (now,),
    ).fetchone()
    if row is None:
        conn.commit()
        return None
    learner_id = row[0]
    row = conn.execute(
        """
        UPDATE jobs SET state = 'running', fence = fence + 1, dirty = 0,
            lease_owner = ?, lease_expires = ?, updated_at = ?
        WHERE learner_id = ? AND state = 'pending'
        RETURNING fence
        """,
        (worker_id, now + lease_seconds, now, learner_id),
    ).fetchone()
    conn.commit()
    if row is None:
        return None
    return Claim(learner_id=learner_id, fence=row[0])


def commit_success(
    conn: sqlite3.Connection,
    claim: Claim,
    profile: list[TagStat],
    now: float,
) -> bool:
    """Atomically replace the profile and clear/requeue the job.

    Returns False (and writes nothing) if the claim was lost to another
    worker while this one was computing.
    """
    conn.execute("BEGIN IMMEDIATE")
    try:
        row = conn.execute(
            "SELECT dirty FROM jobs WHERE learner_id = ? AND fence = ? AND state = 'running'",
            (claim.learner_id, claim.fence),
        ).fetchone()
        if row is None:
            conn.rollback()
            return False
        dirty = row[0]
        conn.execute("DELETE FROM profiles WHERE learner_id = ?", (claim.learner_id,))
        conn.executemany(
            "INSERT INTO profiles (learner_id, tag, mastered_count, max_difficulty)"
            " VALUES (?, ?, ?, ?)",
            [
                (claim.learner_id, s.tag, s.mastered_count, int(s.max_difficulty))
                for s in profile
            ],
        )
        if dirty:
            conn.execute(
                """
                UPDATE jobs SET state = 'pending', attempts = 0, dirty = 0,
                    lease_owner = NULL, lease_expires = NULL, not_before = ?,
                    last_error = NULL, updated_at = ?
                WHERE learner_id = ?
                """,
                (now, now, claim.learner_id),
            )
        else:
            conn.execute("DELETE FROM jobs WHERE learner_id = ?", (claim.learner_id,))
        conn.commit()
        return True
    except BaseException:
        conn.rollback()
        raise


def fail(
    conn: sqlite3.Connection,
    claim: Claim,
    error: str,
    now: float,
    *,
    base_backoff: float = BASE_BACKOFF_SECONDS,
    max_backoff: float = MAX_BACKOFF_SECONDS,
    max_attempts: int = MAX_ATTEMPTS,
) -> None:
    """Record a failed attempt with backoff, or move to dead letters."""
    row = conn.execute(
        "SELECT attempts FROM jobs WHERE learner_id = ? AND fence = ? AND state = 'running'",
        (claim.learner_id, claim.fence),
    ).fetchone()
    if row is None:
        conn.commit()
        return
    new_attempts = row[0] + 1
    if new_attempts >= max_attempts:
        conn.execute(
            """
            UPDATE jobs SET state = 'dead', attempts = ?, dirty = 0,
                lease_owner = NULL, lease_expires = NULL,
                last_error = ?, updated_at = ?
            WHERE learner_id = ?
            """,
            (new_attempts, error, now, claim.learner_id),
        )
    else:
        conn.execute(
            """
            UPDATE jobs SET state = 'pending', attempts = ?, dirty = 0,
                lease_owner = NULL, lease_expires = NULL, not_before = ?,
                last_error = ?, updated_at = ?
            WHERE learner_id = ?
            """,
            (
                new_attempts,
                now + _backoff(new_attempts, base_backoff, max_backoff),
                error,
                now,
                claim.learner_id,
            ),
        )
    conn.commit()


def list_dead(conn: sqlite3.Connection) -> list[DeadLetter]:
    """List learners in the dead-letter state."""
    rows = conn.execute(
        "SELECT learner_id, attempts, COALESCE(last_error, '') FROM jobs"
        " WHERE state = 'dead' ORDER BY updated_at"
    ).fetchall()
    return [DeadLetter(learner_id=r[0], attempts=r[1], last_error=r[2]) for r in rows]


def revive(conn: sqlite3.Connection, learner_id: str, now: float) -> bool:
    """Move a dead-lettered learner back to pending, counter reset."""
    cur = conn.execute(
        """
        UPDATE jobs SET state = 'pending', attempts = 0, dirty = 0,
            lease_owner = NULL, lease_expires = NULL, not_before = ?,
            last_error = NULL, updated_at = ?
        WHERE learner_id = ? AND state = 'dead'
        """,
        (now, now, learner_id),
    )
    conn.commit()
    return cur.rowcount > 0
