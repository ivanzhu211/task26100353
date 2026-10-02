"""Business-rule tests: mastery, tags, difficulty, article edits."""
from __future__ import annotations

import pytest

from skillprofile import Difficulty, TagStat, connect, get_profile
from skillprofile.core import record_completion, upsert_article
from skillprofile.worker import run_worker


@pytest.fixture()
def conn(tmp_path):
    c = connect(str(tmp_path / "test.db"))
    yield c
    c.close()


def drain(conn):
    run_worker(conn.execute("PRAGMA database_list").fetchone()[2], once=True,
               poll_interval=0.01)


def test_no_quiz_completion_masters_article(conn):
    upsert_article(conn, "a1", Difficulty.BEGINNER, ["safety"])
    record_completion(conn, "L1", "a1")
    drain(conn)
    assert get_profile(conn, "L1") == [
        TagStat("safety", 1, Difficulty.BEGINNER)
    ]


def test_score_threshold(conn):
    upsert_article(conn, "a1", Difficulty.EXPERT, ["relay"])
    record_completion(conn, "L1", "a1", score=79)
    drain(conn)
    assert get_profile(conn, "L1") == []
    record_completion(conn, "L1", "a1", score=80)
    drain(conn)
    assert get_profile(conn, "L1") == [TagStat("relay", 1, Difficulty.EXPERT)]


def test_multiple_tags_and_max_difficulty(conn):
    upsert_article(conn, "a1", Difficulty.BEGINNER, ["safety", "relay"])
    upsert_article(conn, "a2", Difficulty.EXPERT, ["safety"])
    record_completion(conn, "L1", "a1")
    record_completion(conn, "L1", "a2", score=95)
    drain(conn)
    assert get_profile(conn, "L1") == [
        TagStat("relay", 1, Difficulty.BEGINNER),
        TagStat("safety", 2, Difficulty.EXPERT),
    ]


def test_untagged_articles_do_not_break_recompute(conn):
    upsert_article(conn, "a1", Difficulty.BEGINNER, [])
    record_completion(conn, "L1", "a1")
    drain(conn)
    assert get_profile(conn, "L1") == []


def test_article_edit_requeues_learners(conn):
    upsert_article(conn, "a1", Difficulty.BEGINNER, ["old"])
    record_completion(conn, "L1", "a1")
    record_completion(conn, "L2", "a1", score=50)
    drain(conn)
    assert get_profile(conn, "L1") == [TagStat("old", 1, Difficulty.BEGINNER)]
    upsert_article(conn, "a1", Difficulty.EXPERT, ["new"])
    drain(conn)
    assert get_profile(conn, "L1") == [TagStat("new", 1, Difficulty.EXPERT)]
    assert get_profile(conn, "L2") == []  # 50 < 80, never mastered


def test_pending_learner_not_enqueued_twice(conn):
    upsert_article(conn, "a1", Difficulty.BEGINNER, ["t"])
    record_completion(conn, "L1", "a1")
    record_completion(conn, "L1", "a1")
    row = conn.execute("SELECT COUNT(*) FROM jobs WHERE learner_id='L1'").fetchone()
    assert row[0] == 1
