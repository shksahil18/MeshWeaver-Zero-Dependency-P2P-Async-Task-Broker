import asyncio

from meshweaver.node import MeshNode


async def main():

    node = MeshNode(
        host="127.0.0.1",
        port=9002,
    )

    await node.start()

    print("Receiver is waiting for messages...")
    print("Press CTRL+C to stop.")
    print()

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
        print("\nReceiver stopped.")