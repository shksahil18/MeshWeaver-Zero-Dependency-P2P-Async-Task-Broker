import asyncio

from meshweaver.node import MeshNode

def add_numbers(a, b):
    """
    Example remote task.
    """

    return a + b


async def main():

    node = MeshNode(
        host="127.0.0.1",
        port=9001,
    )

    await node.start()

    await asyncio.sleep(1)

    # -------------------------
    # Test 1: PING
    # -------------------------

    print("Sending PING...")

    node.ping(
        host="127.0.0.1",
        port=9002,
    )

    await asyncio.sleep(1)

    # -------------------------
    # Test 2: Remote Task
    # -------------------------

    print()
    print(
        "Sending serialized task..."
    )

    node.send_task(
        function=add_numbers,
        args=(10, 20),
        host="127.0.0.1",
        port=9002,
    )

    await asyncio.sleep(5)

    await node.stop()


if __name__ == "__main__":
    asyncio.run(main())