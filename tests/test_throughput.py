"""Acceptance: 4 workers drain a 10k-learner backlog within a minute."""
from __future__ import annotations

import subprocess
import sys
import time

import pytest

from skillprofile import connect
from skillprofile.core import record_completion, upsert_article

N_LEARNERS = 10_000


@pytest.mark.slow
def test_backlog_drain_throughput(tmp_path):
    db_path = str(tmp_path / "perf.db")
    conn = connect(db_path)
    upsert_article(conn, "A1", 2, ["safety", "relay"])
    upsert_article(conn, "A2", 3, ["safety"])
    for i in range(N_LEARNERS):
        record_completion(conn, f"L{i}", "A1", score=90)
        record_completion(conn, f"L{i}", "A2")
    conn.commit()
    backlog = conn.execute("SELECT COUNT(*) FROM jobs").fetchone()[0]
    assert backlog == N_LEARNERS

    workers = [
        subprocess.Popen(
            [sys.executable, "-m", "skillprofile", "--db", db_path,
             "--poll-interval", "0.05"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        for _ in range(4)
    ]
    start = time.monotonic()
    try:
        while time.monotonic() - start < 60:
            remaining = conn.execute("SELECT COUNT(*) FROM jobs").fetchone()[0]
            if remaining == 0:
                break
            time.sleep(0.5)
        elapsed = time.monotonic() - start
    finally:
        for w in workers:
            w.kill()
            w.wait()
    assert remaining == 0, f"{remaining} jobs left after {elapsed:.1f}s"
    sample = conn.execute(
        "SELECT COUNT(*) FROM profiles WHERE learner_id='L0'"
    ).fetchone()[0]
    assert sample == 2
    conn.close()


@pytest.mark.slow
def test_ingest_latency(tmp_path):
    """record_completion < 100ms; editing a 10k-learner article < 1s."""
    db_path = str(tmp_path / "latency.db")
    conn = connect(db_path)
    upsert_article(conn, "HOT", 1, ["t"])
    for i in range(N_LEARNERS):
        record_completion(conn, f"L{i}", "HOT", score=100)

    start = time.monotonic()
    record_completion(conn, "L0", "HOT", score=88)
    assert time.monotonic() - start < 0.1

    start = time.monotonic()
    upsert_article(conn, "HOT", 3, ["t", "t2"])
    assert time.monotonic() - start < 1.0
    conn.close()
