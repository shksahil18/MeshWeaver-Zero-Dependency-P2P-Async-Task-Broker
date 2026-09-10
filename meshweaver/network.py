import asyncio


class MeshUDPProtocol(asyncio.DatagramProtocol):
    """
    Async UDP protocol used by MeshWeaver.
    """

    def __init__(self, on_message, on_connection_lost=None):
        self.on_message = on_message
        self.on_connection_lost = on_connection_lost
        self.transport = None

    def connection_made(self, transport):
        self.transport = transport

    def datagram_received(
        self,
        data,
        addr,
    ):
        self.on_message(
            data,
            addr,
        )

    def error_received(self, exc):
        print(
            f"[NETWORK ERROR] {exc}"
        )

    def connection_lost(self, exc):
        self.transport = None
        if self.on_connection_lost is not None:
            self.on_connection_lost()


class UDPNetwork:
    """
    Asynchronous UDP networking layer.
    """

    def __init__(
        self,
        host: str,
        port: int,
        on_message,
    ):
        self.host = host
        self.port = port
        self.on_message = on_message
        self.transport = None
        self._closed_event = None

    async def start(self):
        loop = asyncio.get_running_loop()
        self._closed_event = asyncio.Event()

        transport, _ = (
            await loop.create_datagram_endpoint(
                lambda: MeshUDPProtocol(
                    self.on_message,
                    self._closed_event.set,
                ),
                local_addr=(
                    self.host,
                    self.port,
                ),
            )
        )

        self.transport = transport

    def send(
        self,
        data: bytes,
        host: str,
        port: int,
    ):
        if self.transport is None:
            raise RuntimeError(
                "UDP network has not been started."
            )

        self.transport.sendto(
            data,
            (host, port),
        )

    async def close(self):
        """Close the transport and wait until the OS releases its socket."""

        transport = self.transport
        closed_event = self._closed_event
        if transport is None:
            return

        transport.close()
        self.transport = None

        # Windows completes UDP close asynchronously. Waiting for
        # connection_lost prevents an immediate restart from raising
        # WinError 10048 for the same local port.
        if closed_event is not None:
            try:
                await asyncio.wait_for(closed_event.wait(), timeout=1.0)
            except asyncio.TimeoutError:
                await asyncio.sleep(0)

        self._closed_event = None
