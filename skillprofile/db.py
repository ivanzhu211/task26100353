"""SQLite connection helpers and schema."""
from __future__ import annotations

import sqlite3

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
CREATE INDEX IF NOT EXISTS idx_completions_learner ON completions (learner_id);
CREATE INDEX IF NOT EXISTS idx_completions_article ON completions (article_id);
CREATE TABLE IF NOT EXISTS profiles (
    learner_id TEXT NOT NULL,
    tag TEXT NOT NULL,
    mastered_count INTEGER NOT NULL,
    max_difficulty INTEGER NOT NULL,
    PRIMARY KEY (learner_id, tag)
);
CREATE TABLE IF NOT EXISTS jobs (
    learner_id TEXT PRIMARY KEY,
    state TEXT NOT NULL CHECK (state IN ('pending', 'running', 'dead')),
    attempts INTEGER NOT NULL DEFAULT 0,
    dirty INTEGER NOT NULL DEFAULT 0,
    fence INTEGER NOT NULL DEFAULT 0,
    lease_owner TEXT,
    lease_expires REAL,
    not_before REAL NOT NULL DEFAULT 0,
    last_error TEXT,
    updated_at REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_jobs_due ON jobs (state, not_before);
"""


def connect(path: str) -> sqlite3.Connection:
    """Open (and initialise) the shared SQLite database."""
    conn = sqlite3.connect(path, timeout=30.0)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.executescript(SCHEMA)
    return conn
