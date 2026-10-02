"""Command-line entry point that starts a recompute worker process."""
from __future__ import annotations

import argparse

from . import queue, worker


def main(argv: list[str] | None = None) -> int:
    """Parse args and run the worker loop."""
    parser = argparse.ArgumentParser(
        prog="skillprofile-worker",
        description="Run a background skill-profile recompute worker.",
    )
    parser.add_argument("--db", required=True, help="path to the shared SQLite file")
    parser.add_argument("--worker-id", default=None, help="defaults to host:pid")
    parser.add_argument(
        "--once",
        action="store_true",
        help="drain currently claimable jobs and exit",
    )
    parser.add_argument("--poll-interval", type=float, default=0.2, metavar="SECONDS")
    parser.add_argument("--lease-seconds", type=float, default=30.0, metavar="SECONDS")
    parser.add_argument("--base-backoff", type=float, default=queue.BASE_BACKOFF_SECONDS, metavar="SECONDS")
    parser.add_argument("--max-backoff", type=float, default=queue.MAX_BACKOFF_SECONDS, metavar="SECONDS")
    args = parser.parse_args(argv)
    worker.run_worker(
        args.db,
        args.worker_id,
        once=args.once,
        poll_interval=args.poll_interval,
        lease_seconds=args.lease_seconds,
        base_backoff=args.base_backoff,
        max_backoff=args.max_backoff,
    )
    return 0
