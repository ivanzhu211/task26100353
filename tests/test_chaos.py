"""Acceptance: concurrent workers, kills, failures, hangs -> consistency."""
from __future__ import annotations

import random
import subprocess
import sys
import threading
import time

import pytest

from skillprofile import connect, get_profile, list_dead
from skillprofile.core import record_completion, upsert_article
from skillprofile.queue import claim, commit_success, fail
from skillprofile.recompute import compute_profile
from skillprofile.worker import run_worker

N_LEARNERS = 60
N_ARTICLES = 12
TAGS = ["safety", "relay", "transformer", "switch", "grounding"]
LEASE = 0.5
BACKOFF = dict(base_backoff=0.05, max_backoff=0.2)


def _start_worker(db_path):
    return subprocess.Popen(
        [sys.executable, "-m", "skillprofile", "--db", db_path,
         "--lease-seconds", str(LEASE), "--poll-interval", "0.02",
         "--base-backoff", "0.05", "--max-backoff", "0.2"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )


@pytest.mark.slow
def test_chaos_eventual_consistency(tmp_path):
    db_path = str(tmp_path / "chaos.db")
    rng = random.Random(20261003)
    learners = [f"L{i}" for i in range(N_LEARNERS)]
    articles = [f"A{i}" for i in range(N_ARTICLES)]

    conn = connect(db_path)
    for a in articles:
        upsert_article(conn, a, rng.randint(1, 3),
                       rng.sample(TAGS, rng.randint(1, 3)))

    # Learners with injected faults: fail twice then recover; one hangs once.
    fail_budget = {f"L{i}": 2 for i in range(0, 10)}
    hang_once = {"L11", "L12"}
    fault_lock = threading.Lock()

    def inject(learner_id):
        with fault_lock:
            if fail_budget.get(learner_id, 0) > 0:
                fail_budget[learner_id] -= 1
                raise RuntimeError("injected failure")
            if learner_id in hang_once:
                hang_once.discard(learner_id)
                time.sleep(LEASE * 3)  # stuck; lease expires, others take over

    stop = threading.Event()

    def faulty_worker():
        c = connect(db_path)
        while not stop.is_set():
            claimed = claim(c, "faulty", time.time(), LEASE, **BACKOFF)
            if claimed is None:
                time.sleep(0.02)
                continue
            try:
                inject(claimed.learner_id)
                profile = compute_profile(c, claimed.learner_id)
            except Exception as exc:
                fail(c, claimed, str(exc), time.time(), **BACKOFF)
            else:
                commit_success(c, claimed, profile, time.time())
        c.close()

    def ingest():
        c = connect(db_path)
        deadline = time.time() + 6.0
        while time.time() < deadline:
            roll = rng.random()
            if roll < 0.75:
                score = rng.choice([None, 50, 79, 80, 95, 100])
                record_completion(c, rng.choice(learners), rng.choice(articles), score)
            else:
                upsert_article(c, rng.choice(articles), rng.randint(1, 3),
                               rng.sample(TAGS, rng.randint(1, 3)))
            time.sleep(0.02)
        c.close()

    workers = [_start_worker(db_path) for _ in range(3)]
    threads = [threading.Thread(target=faulty_worker, daemon=True) for _ in range(2)]
    for t in threads:
        t.start()
    ingester = threading.Thread(target=ingest, daemon=True)
    ingester.start()

    # Randomly kill and respawn subprocess workers while ingest runs.
    while ingester.is_alive():
        time.sleep(rng.uniform(0.3, 0.8))
        idx = rng.randrange(len(workers))
        if workers[idx].poll() is None:
            workers[idx].kill()
            workers[idx].wait()
        workers[idx] = _start_worker(db_path)

    for w in workers:
        w.kill()
        w.wait()
    stop.set()
    for t in threads:
        t.join(timeout=5)

    # Drain whatever is left; only dead letters may remain.
    deadline = time.time() + 30
    while time.time() < deadline:
        run_worker(db_path, once=True, poll_interval=0.02, lease_seconds=LEASE,
                   **BACKOFF)
        remaining = conn.execute(
            "SELECT COUNT(*) FROM jobs WHERE state != 'dead'"
        ).fetchone()[0]
        if remaining == 0:
            break
        time.sleep(0.1)
    else:
        pytest.fail("backlog never drained")

    dead_ids = {d.learner_id for d in list_dead(conn)}
    checked = 0
    for learner in learners:
        if learner in dead_ids:
            continue
        expected = compute_profile(conn, learner)
        assert get_profile(conn, learner) == expected, learner
        checked += 1
    assert checked > 0
    conn.close()
