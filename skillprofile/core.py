"""Ingest-side API: completions, articles, profile reads."""
from __future__ import annotations

import sqlite3
import time

from . import queue
from .types import Difficulty, TagStat


def record_completion(
    conn: sqlite3.Connection,
    learner_id: str,
    article_id: str,
    score: int | None = None,
) -> None:
    """Record one course completion and mark the learner for recompute."""
    now = time.time()
    conn.execute("BEGIN IMMEDIATE")
    try:
        conn.execute(
            "INSERT INTO completions (learner_id, article_id, score, created_at)"
            " VALUES (?, ?, ?, ?)",
            (learner_id, article_id, score, now),
        )
        queue.enqueue(conn, learner_id, now)
        conn.commit()
    except BaseException:
        conn.rollback()
        raise


def upsert_article(
    conn: sqlite3.Connection,
    article_id: str,
    difficulty: int | Difficulty,
    tags: list[str],
) -> None:
    """Create/update an article; affected learners are re-queued."""
    now = time.time()
    conn.execute("BEGIN IMMEDIATE")
    try:
        conn.execute(
            "INSERT INTO articles (article_id, difficulty) VALUES (?, ?)"
            " ON CONFLICT(article_id) DO UPDATE SET difficulty = excluded.difficulty",
            (article_id, int(difficulty)),
        )
        conn.execute("DELETE FROM article_tags WHERE article_id = ?", (article_id,))
        conn.executemany(
            "INSERT INTO article_tags (article_id, tag) VALUES (?, ?)",
            [(article_id, tag) for tag in sorted(set(tags))],
        )
        conn.execute(
            """
            INSERT INTO jobs (learner_id, state, not_before, updated_at)
            SELECT DISTINCT learner_id, 'pending', 0, ?
            FROM completions WHERE article_id = ?
            ON CONFLICT(learner_id) DO UPDATE SET
                dirty = CASE WHEN jobs.state = 'running' THEN 1 ELSE jobs.dirty END,
                updated_at = excluded.updated_at
            """,
            (now, article_id),
        )
        conn.commit()
    except BaseException:
        conn.rollback()
        raise


def get_profile(conn: sqlite3.Connection, learner_id: str) -> list[TagStat]:
    """Return the learner's current committed profile (all-or-nothing)."""
    rows = conn.execute(
        "SELECT tag, mastered_count, max_difficulty FROM profiles"
        " WHERE learner_id = ? ORDER BY tag",
        (learner_id,),
    ).fetchall()
    return [
        TagStat(tag=tag, mastered_count=count, max_difficulty=Difficulty(max_diff))
        for tag, count, max_diff in rows
    ]
