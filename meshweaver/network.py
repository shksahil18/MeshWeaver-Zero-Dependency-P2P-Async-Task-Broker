import asyncio


class MeshUDPProtocol(asyncio.DatagramProtocol):
    """
    Handles incoming and outgoing UDP datagrams.
    """

    def __init__(self, on_message):
        self.on_message = on_message
        self.transport = None

    def connection_made(self, transport):
        self.transport = transport

    def datagram_received(self, data, addr):
        self.on_message(data, addr)

    def error_received(self, exc):
        print(f"[NETWORK ERROR] {exc}")

    def connection_lost(self, exc):
        self.transport = None


class UDPNetwork:
    """
    Async UDP networking layer for MeshWeaver.
    """

    def __init__(self, host: str, port: int, on_message):
        self.host = host
        self.port = port
        self.on_message = on_message
        self.transport = None

    async def start(self):
        """
        Start the UDP endpoint.
        """

        loop = asyncio.get_running_loop()

        transport, _ = await loop.create_datagram_endpoint(
            lambda: MeshUDPProtocol(self.on_message),
            local_addr=(self.host, self.port),
        )

        self.transport = transport

    def send(self, data: bytes, host: str, port: int):
        """
        Send a UDP datagram to another peer.
        """

        if self.transport is None:
            raise RuntimeError(
                "UDP network has not been started."
            )

        self.transport.sendto(
            data,
            (host, port),
        )

    def close(self):
        """
        Close the UDP transport.
        """

        if self.transport is not None:
            self.transport.close()
            self.transport = None