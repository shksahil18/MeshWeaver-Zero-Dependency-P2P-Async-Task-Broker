import asyncio
import uuid

from meshweaver.dht import (
    KademliaNode,
    Peer,
    generate_node_id,
)

from meshweaver.gossip import (
    GossipEngine,
)

from meshweaver.metrics import (
    SystemMetrics,
)

from meshweaver.network import (
    UDPNetwork,
)

from meshweaver.protocol import (
    encode_message,
    decode_message,
)


RPC_TIMEOUT = 3


class MeshNode:
    """
    MeshWeaver Week 2 peer.

    Provides:

    - Async UDP networking
    - Kademlia peer discovery
    - Gossip CPU/RAM exchange
    """

    def __init__(
        self,
        host: str,
        port: int,
    ):

        self.host = host
        self.port = port

        self.node_id = uuid.uuid4().int & (
            (1 << 160) - 1
        )

        self.node_id_hex = (
            f"{self.node_id:040x}"
        )

        self.peer = Peer(
            node_id=self.node_id,
            host=self.host,
            port=self.port,
        )

        self.network = UDPNetwork(
            host=self.host,
            port=self.port,
            on_message=self._on_message,
        )

        self.metrics = SystemMetrics()

        self.dht = KademliaNode(
            local_peer=self.peer,
            send_rpc=self._send_rpc,
        )

        self.gossip = GossipEngine(
            node=self,
            interval=5,
        )

        self.pending_requests = {}

        self.running = False

    async def start(self):
        """
        Start network and Week 2 background services.
        """

        await self.network.start()

        self.running = True

        await self.gossip.start()

        print()
        print("=" * 65)
        print("                 MeshWeaver Node")
        print("=" * 65)
        print(
            f"Node ID : {self.node_id_hex}"
        )
        print(
            f"Address : {self.host}:{self.port}"
        )
        print(
            f"DHT     : {len(self.dht.known_peers())} peers"
        )
        print(
            f"Gossip  : every 5 seconds"
        )
        print("=" * 65)
        print()

    async def stop(self):
        """
        Stop all services.
        """

        self.running = False

        await self.gossip.stop()

        for future in self.pending_requests.values():

            if not future.done():
                future.cancel()

        self.pending_requests.clear()

        self.network.close()

        print(
            f"[NODE] {self.node_id_hex} stopped."
        )

    def _on_message(
        self,
        data: bytes,
        addr,
    ):
        """
        UDP callback.

        Schedule async message processing.
        """

        asyncio.create_task(
            self._handle_message(
                data,
                addr,
            )
        )

    async def _handle_message(
        self,
        data: bytes,
        addr,
    ):
        try:

            message = decode_message(data)

        except Exception as exc:

            print(
                f"[PROTOCOL] Invalid message "
                f"from {addr}: {exc}"
            )

            return

        message_type = message.get(
            "type"
        )

        if message_type == "PING":

            await self._handle_ping(
                message,
                addr,
            )

        elif message_type == "PONG":

            self._handle_pong(
                message,
                addr,
            )

        elif message_type == "FIND_NODE":

            await self._handle_find_node(
                message,
                addr,
            )

        elif message_type == "FIND_NODE_RESPONSE":

            self._handle_rpc_response(
                message
            )

        elif message_type == "GOSSIP":

            self._handle_gossip(
                message,
                addr,
            )

        else:

            print(
                f"[PROTOCOL] Unknown message "
                f"type: {message_type}"
            )

    async def _handle_ping(
        self,
        message,
        addr,
    ):
        """
        Respond to a PING and register the peer.
        """

        sender_id = message.get(
            "node_id"
        )

        if sender_id:

            try:

                peer = Peer(
                    node_id=int(
                        sender_id,
                        16,
                    ),
                    host=addr[0],
                    port=addr[1],
                )

                self.dht.add_peer(peer)

            except (
                ValueError,
                TypeError,
            ):
                pass

        response = {
            "type": "PONG",
            "node_id":
                self.node_id_hex,
        }

        await self.send_message(
            self._peer_from_address(addr),
            response,
        )

    def _handle_pong(
        self,
        message,
        addr,
    ):
        print(
            f"[PING] PONG received "
            f"from {addr[0]}:{addr[1]}"
        )

    async def _handle_find_node(
        self,
        message,
        addr,
    ):
        """
        Kademlia FIND_NODE request handler.
        """

        request_id = message.get(
            "request_id"
        )

        target_id_hex = message.get(
            "target_id"
        )

        if not request_id:
            return

        try:

            target_id = int(
                target_id_hex,
                16,
            )

        except (
            ValueError,
            TypeError,
        ):

            return

        sender_id = message.get(
            "node_id"
        )

        if sender_id:

            try:

                sender_peer = Peer(
                    node_id=int(
                        sender_id,
                        16,
                    ),
                    host=addr[0],
                    port=addr[1],
                )

                self.dht.add_peer(
                    sender_peer
                )

            except (
                ValueError,
                TypeError,
            ):
                pass

        closest = self.dht.handle_find_node(
            target_id
        )

        response = {
            "type":
                "FIND_NODE_RESPONSE",

            "request_id":
                request_id,

            "node_id":
                self.node_id_hex,

            "peers": [
                self.dht.peer_to_record(
                    peer
                )
                for peer in closest
            ],
        }

        await self.send_message(
            self._peer_from_address(addr),
            response,
        )

    def _handle_rpc_response(
        self,
        message,
    ):
        request_id = message.get(
            "request_id"
        )

        future = self.pending_requests.get(
            request_id
        )

        if future is None:
            return

        if not future.done():

            future.set_result(
                message
            )

    def _handle_gossip(
        self,
        message,
        addr,
    ):
        """
        Process remote CPU/RAM metrics.
        """

        sender_id = message.get(
            "node_id"
        )

        if sender_id:

            try:

                peer = Peer(
                    node_id=int(
                        sender_id,
                        16,
                    ),
                    host=addr[0],
                    port=addr[1],
                )

                self.dht.add_peer(peer)

            except (
                ValueError,
                TypeError,
            ):
                pass

        self.gossip.handle_gossip(
            message,
            addr,
        )

    async def _send_rpc(
        self,
        peer: Peer,
        rpc_type: str,
        payload: dict,
    ) -> dict:
        """
        Send a request and wait for a response.
        """

        request_id = str(
            uuid.uuid4()
        )

        message = {
            "type": rpc_type,
            "request_id": request_id,
            "node_id": self.node_id_hex,
            **payload,
        }

        loop = asyncio.get_running_loop()

        future = loop.create_future()

        self.pending_requests[
            request_id
        ] = future

        try:

            await self.send_message(
                peer,
                message,
            )

            return await asyncio.wait_for(
                future,
                timeout=RPC_TIMEOUT,
            )

        except asyncio.TimeoutError as exc:

            raise TimeoutError(
                f"RPC timeout: "
                f"{peer.host}:{peer.port}"
            ) from exc

        finally:

            self.pending_requests.pop(
                request_id,
                None,
            )

    async def send_message(
        self,
        peer: Peer,
        message: dict,
    ):
        """
        Send an encoded message to a peer.
        """

        self.network.send(
            encode_message(message),
            peer.host,
            peer.port,
        )

    def get_gossip_peers(self) -> list[Peer]:
        """
        Return known peers for gossip.
        """

        return self.dht.known_peers()

    def get_peer_metrics(self):
        """
        Return remote CPU/RAM metrics.
        """

        return self.gossip.get_peer_metrics()

    def _peer_from_address(
        self,
        addr,
    ) -> Peer:

        for peer in self.dht.known_peers():

            if peer.address == addr:
                return peer

        return Peer(
            node_id=generate_node_id(
                f"{addr[0]}:{addr[1]}"
            ),
            host=addr[0],
            port=addr[1],
        )

    async def bootstrap(
        self,
        host: str,
        port: int,
    ):
        """
        Join the mesh using a bootstrap peer.
        """

        bootstrap_id = generate_node_id(
            f"{host}:{port}"
        )

        bootstrap_peer = Peer(
            node_id=bootstrap_id,
            host=host,
            port=port,
        )

        await self.dht.bootstrap(
            bootstrap_peer
        )

        self.print_peers()

    def print_peers(self):
        """
        Display currently known DHT peers.
        """

        peers = self.dht.known_peers()

        print()
        print(
            f"[DHT] {len(peers)} known peer(s)"
        )

        for peer in peers:

            print(
                f"  - "
                f"{peer.node_id_hex[:12]}... "
                f"{peer.host}:{peer.port}"
            )

        print()