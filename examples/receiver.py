import argparse
import asyncio

from meshweaver.node import MeshNode
from meshweaver.security import key_from_env


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

    parser.add_argument(
        "--sign",
        action="store_true",
        help="Verify HMAC-SHA256 task signatures using MESHWEAVER_KEY.",
    )

    args = parser.parse_args()

    sign_key = None
    if args.sign:
        sign_key = key_from_env()
        if sign_key is None:
            raise SystemExit(
                "[SECURITY] --sign requires MESHWEAVER_KEY to be set."
            )

    node = MeshNode(
        host=args.host,
        port=args.port,
        sign_key=sign_key,
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
