"""Acceptance tests for the skillrecalc module.

Workers run as real OS processes against a real SQLite file. The oracle
recomputes each profile from raw records with an independent pure-Python
implementation.
"""

from __future__ import annotations

import os
import random
import signal
import sqlite3
import subprocess
import sys
import threading
import time
from pathlib import Path

import pytest

import skillrecalc as sr
from skillrecalc.db import connect
from skillrecalc.worker import Worker

SRC_DIR = Path(__file__).resolve().parent.parent / "src"


# ---------------------------------------------------------------- helpers

def oracle_profile(db: sqlite3.Connection, learner_id: str) -> list[sr.ProfileEntry]:
    """Independent from-scratch recomputation used as the ground truth."""
    difficulty = {}
    for article_id, diff in db.execute("SELECT article_id, difficulty FROM articles"):
        difficulty[article_id] = diff
    tags_by_article: dict[str, list[str]] = {}
    for article_id, tag in db.execute("SELECT article_id, tag FROM article_tags"):
        tags_by_article.setdefault(article_id, []).append(tag)

    mastered: set[str] = set()
    per_article: dict[str, list[int | None]] = {}
    for article_id, score in db.execute(
        "SELECT article_id, score FROM completions WHERE learner_id = ?",
        (learner_id,),
    ):
        per_article.setdefault(article_id, []).append(score)
    for article_id, scores in per_article.items():
        if any(s is None for s in scores) or any(
            s is not None and s >= 80 for s in scores
        ):
            mastered.add(article_id)

    count: dict[str, int] = {}
    max_diff: dict[str, int] = {}
    for article_id in mastered:
        for tag in tags_by_article.get(article_id, []):
            count[tag] = count.get(tag, 0) + 1
            max_diff[tag] = max(max_diff.get(tag, 0), difficulty[article_id])
    return [
        sr.ProfileEntry(tag=t, mastered_count=count[t],
                        max_difficulty=sr.Difficulty(max_diff[t]))
        for t in sorted(count)
    ]


def spawn_worker(db_path: Path, extra_env: dict[str, str] | None = None,
                 extra_args: list[str] | None = None) -> subprocess.Popen:
    env = dict(os.environ)
    env["PYTHONPATH"] = str(SRC_DIR)
    if extra_env:
        env.update(extra_env)
    cmd = [
        sys.executable, "-m", "skillrecalc",
        "--db", str(db_path),
        "--lease-seconds", "1",
        "--backoff-base", "0.02",
        "--poll-interval", "0.05",
    ] + (extra_args or [])
    return subprocess.Popen(
        cmd, env=env,
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )


def wait_until(cond, timeout: float = 30.0, interval: float = 0.05,
               msg: str = "condition not met"):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if cond():
            return
        time.sleep(interval)
    raise TimeoutError(msg)


def queue_size(db: sqlite3.Connection) -> int:
    return db.execute("SELECT count(*) FROM recalc_queue").fetchone()[0]


def stop_workers(workers: list[subprocess.Popen]) -> None:
    for w in workers:
        if w.poll() is None:
            w.send_signal(signal.SIGTERM)
    for w in workers:
        try:
            w.wait(timeout=10)
        except subprocess.TimeoutExpired:
            w.kill()


# ---------------------------------------------------------------- test 1

def test_chaos_never_loses_records(tmp_path):
    """Workers are killed and fail randomly while records and article edits
    keep flowing; afterwards every non-dead-lettered profile must match a
    from-scratch recomputation."""
    rng = random.Random(20261003)
    db_path = tmp_path / "chaos.db"
    sr.init_db(db_path)
    db = connect(db_path)

    tags = ["hv", "relay", "safety", "meter", "ground"]
    articles = [f"a{i}" for i in range(8)]
    learners = [f"L{i}" for i in range(60)]
    for aid in articles:
        sr.upsert_article(
            db, aid, sr.Difficulty(rng.randint(1, 3)),
            rng.sample(tags, rng.randint(1, 2)),
        )
    for lid in learners:
        for _ in range(rng.randint(1, 3)):
            sr.record_completion(db, lid, rng.choice(articles),
                                 rng.choice([None, 60, 90]))

    env = {
        "SKILLRECALC_FAIL_RATE": "0.15",
        "SKILLRECALC_RECOMPUTE_DELAY": "0.02",
    }
    workers = [spawn_worker(db_path, env) for _ in range(4)]
    halt = threading.Event()

    def feeder():
        feed_db = connect(db_path)
        try:
            while not halt.is_set():
                if rng.random() < 0.8:
                    sr.record_completion(
                        feed_db, rng.choice(learners), rng.choice(articles),
                        rng.choice([None, 55, 70, 80, 100]),
                    )
                else:
                    sr.upsert_article(
                        feed_db, rng.choice(articles),
                        sr.Difficulty(rng.randint(1, 3)),
                        rng.sample(tags, rng.randint(1, 2)),
                    )
                time.sleep(rng.uniform(0.005, 0.03))
        finally:
            feed_db.close()

    def killer():
        while not halt.is_set():
            time.sleep(rng.uniform(0.5, 1.2))
            if halt.is_set():
                break
            idx = rng.randrange(len(workers))
            victim = workers[idx]
            if victim.poll() is None:
                victim.send_signal(signal.SIGKILL)
                victim.wait()
            workers[idx] = spawn_worker(db_path, env)

    feed_thread = threading.Thread(target=feeder)
    kill_thread = threading.Thread(target=killer)
    try:
        feed_thread.start()
        kill_thread.start()
        time.sleep(8)
        halt.set()
        feed_thread.join()
        kill_thread.join()

        wait_until(lambda: queue_size(db) == 0, timeout=90,
                   msg="recalc queue did not drain")
    finally:
        halt.set()
        stop_workers(workers)

    dead = {d.learner_id for d in sr.list_dead_letters(db)}
    for lid in learners:
        if lid in dead:
            continue
        assert sr.get_profile(db, lid) == oracle_profile(db, lid), lid


# ---------------------------------------------------------------- test 2

def test_stuck_worker_cannot_overwrite_fresher_profile(tmp_path):
    """A worker stuck after computing wakes up only after another worker
    took over and wrote a newer profile; its stale write must be discarded."""
    db_path = tmp_path / "stuck.db"
    sr.init_db(db_path)
    db = connect(db_path)
    sr.upsert_article(db, "a1", sr.Difficulty.EXPERT, ["hv"])
    sr.upsert_article(db, "a2", sr.Difficulty.BEGINNER, ["meter"])
    sr.record_completion(db, "L1", "a1", 100)

    worker_a = spawn_worker(db_path, {
        "SKILLRECALC_HANG_BEFORE_WRITE_LEARNERS": "L1",
        "SKILLRECALC_HANG_BEFORE_WRITE_SECONDS": "4",
    })
    workers = [worker_a]
    try:
        wait_until(
            lambda: db.execute(
                "SELECT 1 FROM recalc_queue WHERE learner_id='L1' "
                "AND status='running'").fetchone() is not None,
            msg="worker A never claimed L1",
        )
        claimed_at = time.monotonic()

        time.sleep(1.5)  # let A's 1s lease expire while it is stuck
        worker_b = spawn_worker(db_path)
        workers.append(worker_b)

        wait_until(lambda: queue_size(db) == 0,
                   msg="worker B did not finish L1")
        assert [e.tag for e in sr.get_profile(db, "L1")] == ["hv"]

        # New data arrives; B recomputes a fresher profile (v2).
        sr.record_completion(db, "L1", "a2", 95)
        wait_until(lambda: queue_size(db) == 0,
                   msg="worker B did not recompute L1 with new data")
        assert sorted(e.tag for e in sr.get_profile(db, "L1")) == ["hv", "meter"]

        # Wait for A to wake up and attempt its stale write-back.
        remaining = 4.0 - (time.monotonic() - claimed_at)
        if remaining > 0:
            time.sleep(remaining + 1.0)

        assert sr.get_profile(db, "L1") == oracle_profile(db, "L1")
        assert sorted(e.tag for e in sr.get_profile(db, "L1")) == ["hv", "meter"]
    finally:
        stop_workers(workers)


# ---------------------------------------------------------------- test 3

def test_dead_letter_after_exactly_five_failures(tmp_path):
    """Five consecutive failures (one being a killed worker) send the learner
    to the dead-letter table; requeueing resets the count and the profile
    catches up with all records."""
    db_path = tmp_path / "dead.db"
    sr.init_db(db_path)
    db = connect(db_path)
    sr.upsert_article(db, "a1", sr.Difficulty.INTERMEDIATE, ["relay"])
    sr.record_completion(db, "L1", "a1", 88)

    # Failure #1: a real worker process is killed mid-recompute.
    killer_env = {"SKILLRECALC_RECOMPUTE_DELAY": "30"}
    worker = spawn_worker(db_path, killer_env)
    try:
        wait_until(
            lambda: db.execute(
                "SELECT 1 FROM recalc_queue WHERE learner_id='L1' "
                "AND status='running'").fetchone() is not None,
            msg="worker never claimed L1",
        )
        worker.send_signal(signal.SIGKILL)
        worker.wait(timeout=10)
    finally:
        if worker.poll() is None:
            worker.kill()

    os.environ["SKILLRECALC_FAIL_LEARNERS"] = "L1"
    try:
        in_process = Worker(str(db_path), lease_seconds=30, backoff_base=0.01)

        def claimable() -> bool:
            now = time.time()
            return db.execute(
                "SELECT 1 FROM recalc_queue WHERE learner_id='L1' AND ("
                "  (status='pending' AND next_retry_at <= ?) OR"
                "  (status='running' AND lease_expires < ?))",
                (now, now),
            ).fetchone() is not None

        # Round 1 records the kill (takeover) plus an injected failure,
        # rounds 2-4 add one injected failure each: 5 failures in total.
        for round_no in range(1, 5):
            wait_until(claimable, msg=f"L1 not claimable in round {round_no}")
            in_process.run_once(db)
            if round_no < 4:
                assert sr.list_dead_letters(db) == [], (
                    f"dead-lettered too early (round {round_no})"
                )

        dead = sr.list_dead_letters(db)
        assert [d.learner_id for d in dead] == ["L1"]
        assert dead[0].reason
        assert queue_size(db) == 0

        # New records keep the learner in the dead-letter table.
        sr.record_completion(db, "L1", "a1", 99)
        assert [d.learner_id for d in sr.list_dead_letters(db)] == ["L1"]
        assert queue_size(db) == 0

        # Admin puts the learner back; the count restarts and the recalc
        # includes every record accumulated meanwhile.
        sr.requeue_dead_letter(db, "L1")
        assert sr.list_dead_letters(db) == []
    finally:
        del os.environ["SKILLRECALC_FAIL_LEARNERS"]

    in_process.run_once(db)
    assert sr.get_profile(db, "L1") == oracle_profile(db, "L1")
    assert queue_size(db) == 0


# ---------------------------------------------------------------- test 4

def test_performance_baseline(tmp_path):
    """Enqueueing 10k learners via one article edit < 1s, one record < 100ms,
    and 4 workers drain 10k learners well within a minute."""
    n_learners = 10_000
    db_path = tmp_path / "perf.db"
    sr.init_db(db_path)
    db = connect(db_path)
    sr.upsert_article(db, "a1", sr.Difficulty.BEGINNER, ["hv"])

    db.execute("BEGIN IMMEDIATE")
    db.executemany(
        "INSERT INTO completions (learner_id, article_id, score, created_at)"
        " VALUES (?, 'a1', 90, 0)",
        [(f"L{i}",) for i in range(n_learners)],
    )
    db.execute("COMMIT")

    start = time.monotonic()
    sr.upsert_article(db, "a1", sr.Difficulty.EXPERT, ["hv", "safety"])
    enqueue_elapsed = time.monotonic() - start
    assert queue_size(db) == n_learners
    assert enqueue_elapsed < 1.0, f"article update took {enqueue_elapsed:.2f}s"

    start = time.monotonic()
    sr.record_completion(db, "L-extra", "a1", 75)
    record_elapsed = time.monotonic() - start
    assert record_elapsed < 0.1, f"record took {record_elapsed:.3f}s"

    workers = [spawn_worker(db_path, extra_args=["--batch-size", "32"])
               for _ in range(4)]
    try:
        start = time.monotonic()
        wait_until(lambda: queue_size(db) == 0, timeout=60,
                   msg="4 workers could not drain 10k learners in 60s")
        drain_elapsed = time.monotonic() - start
    finally:
        stop_workers(workers)

    assert sr.get_profile(db, "L0") == oracle_profile(db, "L0")
    assert sr.get_profile(db, "L9999") == oracle_profile(db, "L9999")
    print(f"\nenqueue 10k: {enqueue_elapsed:.3f}s, "
          f"record: {record_elapsed*1000:.1f}ms, drain 10k: {drain_elapsed:.1f}s")
