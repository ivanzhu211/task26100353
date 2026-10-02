"""Pure recomputation of a single learner's profile from raw records."""

from __future__ import annotations

import sqlite3

from .api import Difficulty, ProfileEntry


def compute_profile(db: sqlite3.Connection, learner_id: str) -> list[ProfileEntry]:
    """Compute the profile from all current completion records."""
    rows = db.execute(
        """
        WITH mastered AS (
            SELECT article_id
            FROM completions
            WHERE learner_id = ?
            GROUP BY article_id
            HAVING SUM(score IS NULL) > 0 OR MAX(COALESCE(score, -1)) >= 80
        )
        SELECT t.tag, COUNT(*) AS cnt, MAX(a.difficulty) AS max_diff
        FROM mastered m
        JOIN article_tags t ON t.article_id = m.article_id
        JOIN articles a ON a.article_id = m.article_id
        GROUP BY t.tag
        ORDER BY t.tag
        """,
        (learner_id,),
    ).fetchall()
    return [
        ProfileEntry(tag=tag, mastered_count=cnt, max_difficulty=Difficulty(max_diff))
        for tag, cnt, max_diff in rows
    ]
