import argparse
import asyncio

from meshweaver.node import MeshNode


async def main():
    parser = argparse.ArgumentParser(
        description="MeshWeaver Task Receiver"
    )

    parser.add_argument(
        "--host",
        default="127.0.0.1",
        help="Host address",
    )

    parser.add_argument(
        "--port",
        type=int,
        default=9002,
        help="UDP port",
    )

    args = parser.parse_args()

    node = MeshNode(
        host=args.host,
        port=args.port,
    )

    await node.start()

    print("Node is running.")
    print("Press CTRL+C to stop.")

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
        print("\nReceiver stopped.")