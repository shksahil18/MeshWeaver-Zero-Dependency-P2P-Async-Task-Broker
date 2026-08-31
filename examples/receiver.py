import argparse
import asyncio

from meshweaver.node import MeshNode


async def main():
    parser = argparse.ArgumentParser(
        description="MeshWeaver Week 2 Node"
    )

    parser.add_argument(
        "--host",
        default="127.0.0.1",
    )

    parser.add_argument(
        "--port",
        type=int,
        required=True,
    )

    parser.add_argument(
        "--bootstrap",
        default=None,
        help="Bootstrap peer in HOST:PORT format",
    )

    args = parser.parse_args()

    node = MeshNode(
        host=args.host,
        port=args.port,
    )

    await node.start()

    if args.bootstrap:

        host, port = args.bootstrap.split(":")

        await node.bootstrap(
            host,
            int(port),
        )

    print(
        "Node is running."
    )

    print(
        "Press CTRL+C to stop."
    )

    try:

        await asyncio.Event().wait()

    except asyncio.CancelledError:
        pass

    finally:

        await node.stop()


if __name__ == "__main__":

    try:

        asyncio.run(main())

    except KeyboardInterrupt:

        print(
            "\nNode stopped."
        )