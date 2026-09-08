import asyncio
import socket

from meshweaver.node import MeshNode


NODE_COUNT = 10
DISCOVERY_TIMEOUT = 10


def get_local_host():
    """
    Resolve the local host dynamically.
    No peer IP addresses are hardcoded.
    """
    return socket.gethostbyname("localhost")


async def main():
    host = get_local_host()

    nodes = []

    print()
    print("=" * 70)
    print("          MeshWeaver Mid-Review Network Audit")
    print("=" * 70)
    print(f"Target nodes: {NODE_COUNT}")
    print(f"Local host  : {host}")
    print()

    # ------------------------------------------------------------
    # 1. Start 10 nodes using dynamically allocated UDP ports
    # ------------------------------------------------------------

    for index in range(NODE_COUNT):

        # Ask the OS for an available UDP port.
        temp_socket = socket.socket(
            socket.AF_INET,
            socket.SOCK_DGRAM,
        )

        temp_socket.bind((host, 0))

        port = temp_socket.getsockname()[1]

        temp_socket.close()

        node = MeshNode(
            host=host,
            port=port,
        )

        await node.start()

        nodes.append(node)

        print(
            f"[NODE {index + 1:02d}] "
            f"Started on {host}:{port}"
        )

    print()
    print("-" * 70)
    print("All 10 nodes started successfully.")
    print("-" * 70)
    print()

    # ------------------------------------------------------------
    # 2. Bootstrap every node through Node 1
    # ------------------------------------------------------------

    bootstrap_node = nodes[0]

    print(
        f"[BOOTSTRAP] Node 1 is the temporary bootstrap node:"
    )
    print(
        f"[BOOTSTRAP] {bootstrap_node.host}:"
        f"{bootstrap_node.port}"
    )
    print()

    for index, node in enumerate(nodes[1:], start=2):

        print(
            f"[DISCOVERY] Node {index:02d} joining mesh..."
        )

        await node.bootstrap(
            bootstrap_node.host,
            bootstrap_node.port,
        )

    # ------------------------------------------------------------
    # 3. Give DHT discovery time to propagate
    # ------------------------------------------------------------

    print()
    print(
        "[DISCOVERY] Waiting for peer discovery to stabilize..."
    )

    await asyncio.sleep(3)

    # ------------------------------------------------------------
    # 4. Verify every node discovered all other nodes
    # ------------------------------------------------------------

    print()
    print("=" * 70)
    print("                 DISCOVERY AUDIT")
    print("=" * 70)

    discovery_passed = True

    for index, node in enumerate(nodes, start=1):

        discovered = len(node.dht.known_peers())
        expected = NODE_COUNT - 1

        status = "PASS" if discovered >= expected else "FAIL"

        if discovered < expected:
            discovery_passed = False

        print(
            f"[{status}] "
            f"Node {index:02d} -> "
            f"{discovered}/{expected} peers discovered"
        )

    # ------------------------------------------------------------
    # 5. Print final result
    # ------------------------------------------------------------

    print()
    print("=" * 70)

    if discovery_passed:
        print(
            "NETWORK AUDIT: PASS"
        )
        print(
            "10 nodes successfully formed a discoverable mesh."
        )
    else:
        print(
            "NETWORK AUDIT: FAIL"
        )
        print(
            "Not every node discovered all 9 peers."
        )

    print("=" * 70)

    # ------------------------------------------------------------
    # 6. Cleanup
    # ------------------------------------------------------------

    print()
    print("[CLEANUP] Stopping all nodes...")

    for node in nodes:
        await node.stop()

    print("[CLEANUP] Complete.")


if __name__ == "__main__":
    try:
        asyncio.run(main())

    except KeyboardInterrupt:
        print("\nAudit interrupted.")