"""Pure recompute: derive one learner's profile from current data."""
from __future__ import annotations

import sqlite3

from .types import Difficulty, TagStat

_MASTERED_ARTICLES = """
SELECT c.article_id, a.difficulty
FROM completions c
JOIN articles a ON a.article_id = c.article_id
WHERE c.learner_id = ?
GROUP BY c.article_id
HAVING SUM(c.score IS NULL) > 0 OR MAX(c.score) >= 80
"""


def compute_profile(conn: sqlite3.Connection, learner_id: str) -> list[TagStat]:
    """Compute the profile from all current completions (source of truth)."""
    mastered = conn.execute(_MASTERED_ARTICLES, (learner_id,)).fetchall()
    stats: dict[str, list[int]] = {}
    for article_id, difficulty in mastered:
        for (tag,) in conn.execute(
            "SELECT tag FROM article_tags WHERE article_id = ?", (article_id,)
        ):
            entry = stats.setdefault(tag, [0, 0])
            entry[0] += 1
            entry[1] = max(entry[1], difficulty)
    return [
        TagStat(tag=tag, mastered_count=count, max_difficulty=Difficulty(max_diff))
        for tag, (count, max_diff) in sorted(stats.items())
    ]
