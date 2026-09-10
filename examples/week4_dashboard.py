"""
Week 4 – Live Dashboard + Security Demo.

Starts a MeshNode with HMAC-SHA256 task signing enabled and opens
the Rich live dashboard.

Usage
─────
    # Terminal 1 – worker node (run with signing key too)
    python examples/receiver.py --port 9002

    # Terminal 2 – dashboard node
    python examples/week4_dashboard.py --port 9001 --worker 127.0.0.1:9002

    # With signing (both sender and receiver must use the same key)
    export MESHWEAVER_KEY=abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789
    python examples/week4_dashboard.py --port 9001 --worker 127.0.0.1:9002 --sign

Optional flags
──────────────
    --sign      Enable HMAC-SHA256 message signing for TASK messages.
                Requires the same MESHWEAVER_KEY used by the worker.

    --tasks N   Submit N background tasks to stress-test routing (default 0).
"""

import argparse
import asyncio
import os
import time

from meshweaver.dashboard import MeshDashboard
from meshweaver.node import MeshNode
from meshweaver.security import key_from_env


# ---------------------------------------------------------------------------
# Sample task functions
# ---------------------------------------------------------------------------


def add(a, b):
    return a + b


def multiply(a, b):
    return a * b


def slow_add(a, b, delay=2):
    time.sleep(delay)
    return a + b


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


async def main():

    parser = argparse.ArgumentParser(
        description="MeshWeaver Week 4 — Dashboard + Security demo"
    )
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=9001)
    parser.add_argument(
        "--worker",
        action="append",
        default=[],
        metavar="HOST:PORT",
        help="Worker address.  Repeat for multiple.  Default: 9002.",
    )
    parser.add_argument(
        "--sign",
        action="store_true",
        help="Enable HMAC-SHA256 task signing.",
    )
    parser.add_argument(
        "--tasks",
        type=int,
        default=0,
        metavar="N",
        help="Submit N background tasks.",
    )
    args = parser.parse_args()

    workers = args.worker or ["127.0.0.1:9002"]

    # ── Security key ──────────────────────────────────────────────────────
    sign_key = None

    if args.sign:
        sign_key = key_from_env()
        if sign_key is None:
            raise SystemExit(
                "[SECURITY] --sign requires MESHWEAVER_KEY to be set "
                "so the worker can use the same shared key."
            )
        print(
            f"[SECURITY] HMAC-SHA256 signing enabled.  "
            f"Key: {sign_key.hex()[:16]}..."
        )

    # ── Start node ────────────────────────────────────────────────────────
    node = MeshNode(
        host=args.host,
        port=args.port,
        sign_key=sign_key,
    )

    try:
        await node.start()
    except OSError as exc:
        if getattr(exc, "winerror", None) == 10048 or getattr(exc, "errno", None) in (48, 98):
            print(
                f"[STARTUP ERROR] UDP port {args.port} is already in use. "
                "Stop the other MeshWeaver process or choose another --port."
            )
            return
        raise

    # ── Register workers ──────────────────────────────────────────────────
    print(f"\n[DEMO] Registering {len(workers)} worker(s)...")
    for w in workers:
        host, port_str = w.rsplit(":", 1)
        node.ping(host, int(port_str))

    # Wait for gossip metrics.
    await asyncio.sleep(6)

    # ── Submit background tasks ───────────────────────────────────────────
    submitted_ids = []

    if args.tasks > 0:
        print(f"\n[DEMO] Submitting {args.tasks} task(s)...")

        funcs = [add, multiply, slow_add]

        for i in range(args.tasks):
            fn = funcs[i % len(funcs)]
            try:
                task_id = await node.route_task(fn, args=(i, i + 1))
                submitted_ids.append(task_id)
            except Exception as exc:
                print(f"[DEMO] Routing error: {exc}")

    # ── Launch dashboard ──────────────────────────────────────────────────
    dashboard = MeshDashboard(node, refresh_interval=1.0)

    try:
        await dashboard.run()

    except KeyboardInterrupt:
        pass

    finally:
        # Print a static summary before exiting.
        print()
        print(node.tracker.format_ledger())
        print()
        await node.stop()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nDashboard stopped.")
