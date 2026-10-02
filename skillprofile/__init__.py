"""Skill-profile background recompute library."""
from __future__ import annotations

from .core import get_profile, record_completion, upsert_article
from .db import connect
from .queue import list_dead, revive
from .types import DeadLetter, Difficulty, TagStat
from .worker import run_worker

__all__ = [
    "DeadLetter",
    "Difficulty",
    "TagStat",
    "connect",
    "get_profile",
    "list_dead",
    "record_completion",
    "revive",
    "run_worker",
    "upsert_article",
]
