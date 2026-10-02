"""Exactly 5 consecutive failures -> dead letter; revive resets and recovers."""
from __future__ import annotations

import time

import pytest

from skillprofile import Difficulty, TagStat, connect, get_profile, list_dead, revive
from skillprofile.core import record_completion, upsert_article
from skillprofile.queue import claim
from skillprofile.recompute import compute_profile
from skillprofile.worker import run_worker

KW = dict(poll_interval=0.01, lease_seconds=30.0, base_backoff=0.01,
          max_backoff=0.05)


@pytest.fixture()
def db_path(tmp_path):
    return str(tmp_path / "test.db")


def drain(db_path, **kw):
    run_worker(db_path, once=True, **{**KW, **kw})


def test_dead_letter_after_exactly_five_failures(db_path):
    conn = connect(db_path)
    upsert_article(conn, "a1", Difficulty.EXPERT, ["relay"])
    record_completion(conn, "L1", "a1", score=99)

    def boom(c, learner_id):
        raise RuntimeError("recompute exploded")

    # Failures 1-4: still pending, not dead, retried with growing backoff.
    for expected_attempts in range(1, 5):
        drain(db_path, compute_fn=boom)
        assert list_dead(conn) == []
        row = conn.execute(
            "SELECT attempts, state FROM jobs WHERE learner_id='L1'"
        ).fetchone()
        assert row == (expected_attempts, "pending")
        time.sleep(0.06)  # let the backoff elapse (cap is 0.05)

    # Failure 5: killed mid-recompute (claimed, never finished, lease expires).
    victim = claim(conn, "doomed-worker", time.time(), lease_seconds=0.01)
    assert victim is not None
    time.sleep(0.02)
    drain(db_path, compute_fn=boom)  # recovery sweep counts the 5th failure

    dead = list_dead(conn)
    assert len(dead) == 1
    assert dead[0].learner_id == "L1"
    assert dead[0].attempts == 5
    assert "lease expired" in dead[0].last_error

    # Dead learners are never claimed again, even when new records arrive.
    record_completion(conn, "L1", "a1")
    drain(db_path)
    assert [d.learner_id for d in list_dead(conn)] == ["L1"]
    assert get_profile(conn, "L1") == []  # never recomputed

    # Admin revives: counter resets, recompute catches up on everything.
    assert revive(conn, "L1", time.time()) is True
    drain(db_path)
    assert list_dead(conn) == []
    assert get_profile(conn, "L1") == [TagStat("relay", 1, Difficulty.EXPERT)]
    assert get_profile(conn, "L1") == compute_profile(conn, "L1")
    conn.close()


def test_success_resets_failure_counter(db_path):
    conn = connect(db_path)
    upsert_article(conn, "a1", Difficulty.BEGINNER, ["t"])
    record_completion(conn, "L1", "a1")

    def boom(c, learner_id):
        raise RuntimeError("flaky")

    for _ in range(4):
        drain(db_path, compute_fn=boom)
        time.sleep(0.06)
    drain(db_path)  # success resets attempts
    assert get_profile(conn, "L1") == [TagStat("t", 1, Difficulty.BEGINNER)]
    assert conn.execute("SELECT COUNT(*) FROM jobs").fetchone()[0] == 0

    # A fresh failure starts counting from 1 again.
    record_completion(conn, "L1", "a1")
    drain(db_path, compute_fn=boom)
    row = conn.execute("SELECT attempts, state FROM jobs").fetchone()
    assert row == (1, "pending")
    conn.close()
