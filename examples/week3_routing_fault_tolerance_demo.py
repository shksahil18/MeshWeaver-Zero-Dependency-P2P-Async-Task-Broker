"""
Week 3 routing + fault-tolerance demonstration.

Start two worker nodes first (in separate terminals):

    python examples/receiver.py --port 9002
    python examples/receiver.py --port 9003

Then run this demo:

    python examples/week3_routing_fault_tolerance_demo.py

Optional flags
──────────────
    --failure-demo   Send a long-running task so you can stop one worker
                     mid-flight and watch the heartbeat detect the failure
                     and re-route automatically.

The demo:
  1. Registers two workers via PING.
  2. Waits for Gossip CPU/RAM reports (6 seconds).
  3. Prints the routing table (shows which node has lower CPU).
  4. Routes a task to the lowest-load worker via ``node.route_task()``.
  5. Waits for the final result or a re-route event.
"""

import argparse
import asyncio
import time

from meshweaver.node import MeshNode


# ---------------------------------------------------------------------------
# Sample functions serialized and sent to a remote worker
# ---------------------------------------------------------------------------


def calculate(left, right):
    """Normal task: trivial addition."""
    return left + right


def slow_calculate(left, right, seconds=12):
    """
    Long-running task used for the fault-tolerance demo.

    After dispatching this task, stop the selected worker process
    (CTRL+C on that terminal).  The sender will detect the missed
    heartbeats within ~6 s and automatically re-route to the other
    worker.
    """
    time.sleep(seconds)
    return left + right


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


async def main():
    parser = argparse.ArgumentParser(
        description="MeshWeaver Week 3 routing + fault-tolerance demo"
    )
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=9001)
    parser.add_argument(
        "--worker",
        action="append",
        default=[],
        metavar="HOST:PORT",
        help="Worker address.  Repeat for multiple.  Default: 9002 and 9003.",
    )
    parser.add_argument(
        "--failure-demo",
        action="store_true",
        help="Send a 12-second task so a worker can be killed mid-flight.",
    )
    args = parser.parse_args()

    workers = args.worker or ["127.0.0.1:9002", "127.0.0.1:9003"]

    node = MeshNode(host=args.host, port=args.port)
    await node.start()

    try:
        # ── Step 1: register workers ───────────────────────────────────────
        print("\n[DEMO] Pinging workers...")
        for w in workers:
            host, port_str = w.rsplit(":", 1)
            node.ping(host, int(port_str))

        await asyncio.sleep(2)

        # ── Step 2: wait for gossip metrics ────────────────────────────────
        print("\n[DEMO] Waiting for Gossip CPU/RAM reports (6 s)...")
        await asyncio.sleep(6)

        # ── Step 3: print routing table ────────────────────────────────────
        print()
        print(node.router.format_table())
        print()

        # ── Step 4: submit task ────────────────────────────────────────────
        if args.failure_demo:
            print(
                "[DEMO] Sending slow_calculate(10, 20, seconds=12).\n"
                "[DEMO] Stop the selected worker now (CTRL+C in that terminal).\n"
                "[DEMO] Heartbeat will detect failure and re-route within ~6 s.\n"
            )
            task_id = await node.route_task(
                slow_calculate,
                args=(10, 20, 12),
            )
        else:
            print("[DEMO] Sending calculate(10, 20)...")
            task_id = await node.route_task(
                calculate,
                args=(10, 20),
            )

        print(f"[DEMO] Task submitted: {task_id}")

        # ── Step 5: watch task state ───────────────────────────────────────
        print("[DEMO] Waiting for result (max 60 s)...\n")

        try:
            result = await node.wait_for_result(task_id, timeout=60.0)
            print(f"\n[DEMO] ✓  Task completed!  Result = {result}")
        except Exception as exc:
            print(f"\n[DEMO] ✗  Task failed: {exc}")

        # Print final task history.
        record = node.tracker.get(task_id)
        if record:
            print("\n[DEMO] Task history:")
            for line in record.history:
                print(f"   {line}")

    finally:
        await node.stop()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nWeek 3 demo stopped.")
