import argparse
import asyncio

from meshweaver.node import MeshNode


def add(left, right):
    """Add two numbers."""
    return left + right


async def main():
    parser = argparse.ArgumentParser(
        description="MeshWeaver Task Sender"
    )

    parser.add_argument(
        "--host",
        default="127.0.0.1",
        help="Sender host address",
    )

    parser.add_argument(
        "--port",
        type=int,
        default=9001,
        help="Sender UDP port",
    )

    parser.add_argument(
        "--peer-host",
        default="127.0.0.1",
        help="Receiver host address",
    )

    parser.add_argument(
        "--peer-port",
        type=int,
        default=9002,
        help="Receiver UDP port",
    )

    args = parser.parse_args()

    node = MeshNode(
        host=args.host,
        port=args.port,
    )

    await node.start()

    print()
    print("[SENDER] Connecting to receiver...")
    print(
        f"[SENDER] Receiver: "
        f"{args.peer_host}:{args.peer_port}"
    )

    # Add receiver to DHT by sending PING.
    node.ping(
        args.peer_host,
        args.peer_port,
    )

    # Give receiver time to respond.
    await asyncio.sleep(1)

    print()
    print("[TASK] Sending: 10 + 20")

    task_id = node.send_task(
        add,
        args=(10, 20),
        kwargs={},
        host=args.peer_host,
        port=args.peer_port,
    )

    print(
        f"[TASK] Task ID: {task_id}"
    )

    print()
    print("[SENDER] Waiting for TASK_RESULT...")
    print("[SENDER] Press CTRL+C to stop.")

    try:
        while True:
            await asyncio.sleep(3600)

    except asyncio.CancelledError:
        pass

    finally:
        await node.stop()


if __name__ == "__main__":
    try:
        asyncio.run(main())

    except KeyboardInterrupt:
        print("\nSender stopped.")