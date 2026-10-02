"""A stuck worker that wakes after takeover must not overwrite newer results."""
from __future__ import annotations

import time

import pytest

from skillprofile import Difficulty, TagStat, connect, get_profile
from skillprofile.core import record_completion, upsert_article
from skillprofile.queue import claim, commit_success
from skillprofile.recompute import compute_profile


@pytest.fixture()
def conn(tmp_path):
    c = connect(str(tmp_path / "test.db"))
    yield c
    c.close()


def test_stuck_worker_cannot_overwrite_takeover_result(conn):
    upsert_article(conn, "a1", Difficulty.BEGINNER, ["t1"])
    record_completion(conn, "L1", "a1")

    now = time.time()
    stuck = claim(conn, "worker-stuck", now, lease_seconds=0.05)
    assert stuck is not None
    stale_profile = compute_profile(conn, stuck.learner_id)  # only knows t1

    # New data arrives while the stuck worker is "hung".
    upsert_article(conn, "a2", Difficulty.EXPERT, ["t2"])
    record_completion(conn, "L1", "a2", score=100)

    # Lease expires; another worker takes over and commits fresh results.
    time.sleep(0.06)
    takeover = claim(conn, "worker-healthy", time.time(), lease_seconds=30.0,
                 base_backoff=0.0)
    assert takeover is not None and takeover.learner_id == "L1"
    assert takeover.fence != stuck.fence
    fresh_profile = compute_profile(conn, takeover.learner_id)
    assert commit_success(conn, takeover, fresh_profile, time.time()) is True

    # The stuck worker finally wakes up and tries to write its stale result.
    assert commit_success(conn, stuck, stale_profile, time.time()) is False

    assert get_profile(conn, "L1") == [
        TagStat("t1", 1, Difficulty.BEGINNER),
        TagStat("t2", 1, Difficulty.EXPERT),
    ]


def test_expired_lease_counts_as_failure(conn):
    upsert_article(conn, "a1", Difficulty.BEGINNER, ["t1"])
    record_completion(conn, "L1", "a1")
    now = time.time()
    assert claim(conn, "worker-doomed", now, lease_seconds=0.05) is not None
    time.sleep(0.06)
    # Recovery sweep runs inside the next claim attempt.
    assert claim(conn, "worker-healthy", time.time(), 30.0) is None  # backoff
    row = conn.execute(
        "SELECT attempts, state, last_error FROM jobs WHERE learner_id='L1'"
    ).fetchone()
    assert row[0] == 1 and row[1] == "pending"
    assert "lease expired" in row[2]
