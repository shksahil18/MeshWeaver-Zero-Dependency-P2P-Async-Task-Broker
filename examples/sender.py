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

        while True:

            await asyncio.sleep(10)

            print()
            print(
                "[STATUS] Known peers:"
            )

            node.print_peers()

            print(
                "[STATUS] Received metrics:"
            )

            for (
                node_id,
                metrics,
            ) in node.get_peer_metrics().items():

                print(
                    f"  {node_id:040x} "
                    f"CPU="
                    f"{metrics['cpu_percent']:.2f}% "
                    f"RAM="
                    f"{metrics['memory_percent']:.2f}%"
                )

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