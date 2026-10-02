"""Command-line entry point that starts a recalculation worker process."""

from __future__ import annotations

import argparse

from .api import init_db
from .worker import (
    DEFAULT_BACKOFF_BASE,
    DEFAULT_LEASE_SECONDS,
    DEFAULT_MAX_ATTEMPTS,
    Worker,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="skillrecalc-worker",
        description="Run a background skill-profile recalculation worker.",
    )
    parser.add_argument("--db", required=True, help="path to the SQLite file")
    parser.add_argument("--worker-id", default=None)
    parser.add_argument("--lease-seconds", type=float, default=DEFAULT_LEASE_SECONDS)
    parser.add_argument("--max-attempts", type=int, default=DEFAULT_MAX_ATTEMPTS)
    parser.add_argument("--backoff-base", type=float, default=DEFAULT_BACKOFF_BASE)
    parser.add_argument("--batch-size", type=int, default=16)
    parser.add_argument("--poll-interval", type=float, default=0.2)
    parser.add_argument(
        "--once",
        action="store_true",
        help="stop when the recalc queue is empty instead of running forever",
    )
    args = parser.parse_args(argv)

    init_db(args.db)
    worker = Worker(
        args.db,
        worker_id=args.worker_id,
        lease_seconds=args.lease_seconds,
        max_attempts=args.max_attempts,
        backoff_base=args.backoff_base,
        batch_size=args.batch_size,
        poll_interval=args.poll_interval,
    )
    worker.run_forever(stop_after_empty=args.once)
    return 0
