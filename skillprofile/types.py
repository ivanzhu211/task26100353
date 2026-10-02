"""Shared types for the skill-profile recompute module."""
from __future__ import annotations

from dataclasses import dataclass
from enum import IntEnum


class Difficulty(IntEnum):
    """Article difficulty, ordered so max() picks the hardest."""

    BEGINNER = 1
    INTERMEDIATE = 2
    EXPERT = 3


@dataclass(frozen=True)
class TagStat:
    """One row of a learner's skill profile."""

    tag: str
    mastered_count: int
    max_difficulty: Difficulty


@dataclass(frozen=True)
class DeadLetter:
    """A learner whose recompute repeatedly failed."""

    learner_id: str
    attempts: int
    last_error: str
