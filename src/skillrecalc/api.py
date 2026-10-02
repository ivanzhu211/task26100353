"""Public library API used by the training platform's upper-layer code."""

from __future__ import annotations

import enum
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path

from .db import connect, ensure_schema, transaction


class Difficulty(enum.IntEnum):
    BEGINNER = 1
    INTERMEDIATE = 2
    EXPERT = 3


@dataclass(frozen=True)
class ProfileEntry:
    tag: str
    mastered_count: int
    max_difficulty: Difficulty


@dataclass(frozen=True)
class DeadLetter:
    learner_id: str
    reason: str
    failed_at: float


def init_db(path: str | Path) -> None:
    """Create the database file and schema if needed."""
    conn = connect(path)
    try:
        ensure_schema(conn)
    finally:
        conn.close()


def _enqueue_learners(db: sqlite3.Connection, learner_ids: list[str]) -> None:
    """Mark learners dirty. Must be called inside a transaction.

    - learners already waiting are left untouched (no extra recalc);
    - learners being recomputed get needs_rerun=1 (at most one extra run);
    - dead-lettered learners stay in the dead-letter table.
    """
    if not learner_ids:
        return
    now = time.time()
    db.executemany(
        """
        INSERT OR IGNORE INTO recalc_queue
            (learner_id, status, attempts, next_retry_at, needs_rerun, updated_at)
        SELECT ?, 'pending', 0, 0, 0, ?
        WHERE NOT EXISTS (SELECT 1 FROM dead_letters WHERE learner_id = ?)
        """,
        [(lid, now, lid) for lid in learner_ids],
    )
    db.executemany(
        """
        UPDATE recalc_queue SET needs_rerun = 1, updated_at = ?
        WHERE learner_id = ? AND status = 'running'
        """,
        [(now, lid) for lid in learner_ids],
    )


def _enqueue_by_article(db: sqlite3.Connection, article_id: str) -> None:
    """Bulk-enqueue every learner who has a completion for the article."""
    now = time.time()
    db.execute(
        """
        INSERT OR IGNORE INTO recalc_queue
            (learner_id, status, attempts, next_retry_at, needs_rerun, updated_at)
        SELECT DISTINCT c.learner_id, 'pending', 0, 0, 0, ?
        FROM completions c
        WHERE c.article_id = ?
          AND NOT EXISTS (
              SELECT 1 FROM dead_letters d WHERE d.learner_id = c.learner_id)
        """,
        (now, article_id),
    )
    db.execute(
        """
        UPDATE recalc_queue SET needs_rerun = 1, updated_at = ?
        WHERE status = 'running' AND learner_id IN (
            SELECT learner_id FROM completions WHERE article_id = ?)
        """,
        (now, article_id),
    )


def upsert_article(
    db: sqlite3.Connection,
    article_id: str,
    difficulty: Difficulty,
    tags: list[str],
) -> None:
    """Create or update an article; re-enqueues affected learners on change."""
    with transaction(db):
        db.execute(
            """
            INSERT INTO articles (article_id, difficulty) VALUES (?, ?)
            ON CONFLICT(article_id) DO UPDATE SET difficulty = excluded.difficulty
            """,
            (article_id, int(difficulty)),
        )
        db.execute("DELETE FROM article_tags WHERE article_id = ?", (article_id,))
        db.executemany(
            "INSERT OR IGNORE INTO article_tags (article_id, tag) VALUES (?, ?)",
            [(article_id, tag) for tag in tags],
        )
        _enqueue_by_article(db, article_id)


def record_completion(
    db: sqlite3.Connection,
    learner_id: str,
    article_id: str,
    score: int | None = None,
) -> None:
    """Record one completion and mark the learner dirty. Must be fast."""
    if score is not None and not 0 <= score <= 100:
        raise ValueError("score must be within 0..100")
    with transaction(db):
        db.execute(
            """
            INSERT INTO completions (learner_id, article_id, score, created_at)
            VALUES (?, ?, ?, ?)
            """,
            (learner_id, article_id, score, time.time()),
        )
        _enqueue_learners(db, [learner_id])


def get_profile(db: sqlite3.Connection, learner_id: str) -> list[ProfileEntry]:
    """Return the learner's current profile entries (empty if none)."""
    rows = db.execute(
        """
        SELECT tag, mastered_count, max_difficulty
        FROM profiles WHERE learner_id = ? ORDER BY tag
        """,
        (learner_id,),
    ).fetchall()
    return [
        ProfileEntry(tag=tag, mastered_count=cnt, max_difficulty=Difficulty(d))
        for tag, cnt, d in rows
    ]


def list_dead_letters(db: sqlite3.Connection) -> list[DeadLetter]:
    rows = db.execute(
        "SELECT learner_id, reason, failed_at FROM dead_letters ORDER BY failed_at"
    ).fetchall()
    return [DeadLetter(learner_id=lid, reason=r, failed_at=t) for lid, r, t in rows]


def requeue_dead_letter(db: sqlite3.Connection, learner_id: str) -> None:
    """Move a learner out of the dead-letter table back into the queue."""
    with transaction(db):
        cur = db.execute("DELETE FROM dead_letters WHERE learner_id = ?", (learner_id,))
        if cur.rowcount == 0:
            raise KeyError(f"learner {learner_id!r} is not in the dead-letter table")
        _enqueue_learners(db, [learner_id])
