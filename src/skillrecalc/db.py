"""SQLite connection helpers and schema."""

from __future__ import annotations

import sqlite3
import contextlib
from pathlib import Path

SCHEMA = """
CREATE TABLE IF NOT EXISTS articles (
    article_id TEXT PRIMARY KEY,
    difficulty INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS article_tags (
    article_id TEXT NOT NULL,
    tag TEXT NOT NULL,
    PRIMARY KEY (article_id, tag)
);
CREATE TABLE IF NOT EXISTS completions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    learner_id TEXT NOT NULL,
    article_id TEXT NOT NULL,
    score INTEGER,
    created_at REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_completions_learner ON completions(learner_id);
CREATE INDEX IF NOT EXISTS idx_completions_article ON completions(article_id);

CREATE TABLE IF NOT EXISTS profiles (
    learner_id TEXT NOT NULL,
    tag TEXT NOT NULL,
    mastered_count INTEGER NOT NULL,
    max_difficulty INTEGER NOT NULL,
    PRIMARY KEY (learner_id, tag)
);

CREATE TABLE IF NOT EXISTS recalc_queue (
    learner_id TEXT PRIMARY KEY,
    status TEXT NOT NULL DEFAULT 'pending',
    attempts INTEGER NOT NULL DEFAULT 0,
    next_retry_at REAL NOT NULL DEFAULT 0,
    needs_rerun INTEGER NOT NULL DEFAULT 0,
    lease_owner TEXT,
    lease_token TEXT,
    lease_expires REAL,
    updated_at REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_queue_pending
    ON recalc_queue(status, next_retry_at);
CREATE INDEX IF NOT EXISTS idx_queue_lease
    ON recalc_queue(status, lease_expires);

CREATE TABLE IF NOT EXISTS dead_letters (
    learner_id TEXT PRIMARY KEY,
    reason TEXT NOT NULL,
    failed_at REAL NOT NULL
);
"""


def connect(path: str | Path) -> sqlite3.Connection:
    """Open a connection with WAL and busy-timeout configured."""
    conn = sqlite3.connect(str(path), timeout=30.0, isolation_level=None)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=30000")
    return conn


def ensure_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA)


@contextlib.contextmanager
def transaction(conn: sqlite3.Connection):
    """Explicit BEGIN IMMEDIATE transaction (connections run in autocommit)."""
    conn.execute("BEGIN IMMEDIATE")
    try:
        yield conn
    except BaseException:
        conn.execute("ROLLBACK")
        raise
    else:
        conn.execute("COMMIT")
