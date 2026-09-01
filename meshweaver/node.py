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

from meshweaver.tasks.serializer import (
    serialize_task,
    deserialize_task,
)


RPC_TIMEOUT = 3


class MeshNode:
    """
    MeshWeaver peer node.

    Features:
        - Async UDP networking
        - PING / PONG communication
        - Kademlia peer discovery
        - Gossip CPU/RAM exchange
        - Cloudpickle task serialization
        - Remote task execution
        - TASK_RESULT responses
    """

    def __init__(
        self,
        host: str,
        port: int,
    ):
        self.host = host
        self.port = port

        # --------------------------------------------------------
        # Generate a unique 160-bit node ID
        # --------------------------------------------------------

        self.node_id = uuid.uuid4().int & (
            (1 << 160) - 1
        )

        self.node_id_hex = (
            f"{self.node_id:040x}"
        )

        # --------------------------------------------------------
        # Local peer information
        # --------------------------------------------------------

        self.peer = Peer(
            node_id=self.node_id,
            host=self.host,
            port=self.port,
        )

        # --------------------------------------------------------
        # UDP networking layer
        # --------------------------------------------------------

        self.network = UDPNetwork(
            host=self.host,
            port=self.port,
            on_message=self._on_message,
        )

        # --------------------------------------------------------
        # System metrics
        # --------------------------------------------------------

        self.metrics = SystemMetrics()

        # --------------------------------------------------------
        # Kademlia DHT
        # --------------------------------------------------------

        self.dht = KademliaNode(
            local_peer=self.peer,
            send_rpc=self._send_rpc,
        )

        # --------------------------------------------------------
        # Gossip engine
        # --------------------------------------------------------

        self.gossip = GossipEngine(
            node=self,
            interval=5,
        )

        # --------------------------------------------------------
        # Pending DHT RPC requests
        # --------------------------------------------------------

        self.pending_requests = {}

        # --------------------------------------------------------
        # Node state
        # --------------------------------------------------------

        self.running = False

    # ============================================================
    # NODE LIFECYCLE
    # ============================================================

    async def start(self):
        """
        Start the MeshWeaver node.
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
            "Gossip  : every 5 seconds"
        )
        print("=" * 65)
        print()

    async def stop(self):
        """
        Stop the MeshWeaver node.
        """

        self.running = False

        await self.gossip.stop()

        # Cancel pending DHT requests.
        for future in self.pending_requests.values():

            if not future.done():
                future.cancel()

        self.pending_requests.clear()

        self.network.close()

        print(
            f"[NODE] {self.node_id_hex} stopped."
        )

    # ============================================================
    # INCOMING UDP MESSAGE HANDLING
    # ============================================================

    def _on_message(
        self,
        data: bytes,
        addr,
    ):
        """
        UDP callback.

        Creates an asynchronous task for processing
        the received message.
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
        """
        Decode and route an incoming protocol message.
        """

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

        # --------------------------------------------------------
        # PING
        # --------------------------------------------------------

        if message_type == "PING":

            await self._handle_ping(
                message,
                addr,
            )

        # --------------------------------------------------------
        # PONG
        # --------------------------------------------------------

        elif message_type == "PONG":

            self.handle_pong(
                message,
                addr,
            )

        # --------------------------------------------------------
        # FIND_NODE
        # --------------------------------------------------------

        elif message_type == "FIND_NODE":

            await self._handle_find_node(
                message,
                addr,
            )

        # --------------------------------------------------------
        # FIND_NODE_RESPONSE
        # --------------------------------------------------------

        elif message_type == "FIND_NODE_RESPONSE":

            self._handle_rpc_response(
                message
            )

        # --------------------------------------------------------
        # GOSSIP
        # --------------------------------------------------------

        elif message_type == "GOSSIP":

            self._handle_gossip(
                message,
                addr,
            )

        # --------------------------------------------------------
        # TASK
        # --------------------------------------------------------

        elif message_type == "TASK":

            await self.handle_task(
                message,
                addr,
            )

        # --------------------------------------------------------
        # TASK_RESULT
        # --------------------------------------------------------

        elif message_type == "TASK_RESULT":

            self.handle_task_result(
                message,
                addr,
            )

        else:

            print(
                f"[PROTOCOL] Unknown message "
                f"type: {message_type}"
            )

    # ============================================================
    # PING / PONG
    # ============================================================

    def ping(
        self,
        host: str,
        port: int,
    ):
        """
        Send a PING message to another MeshWeaver node.

        This method is used by ui/app.py.
        """

        if not self.running:

            raise RuntimeError(
                "Mesh node is not running."
            )

        # Create a lightweight peer representation.
        peer = Peer(
            node_id=generate_node_id(
                f"{host}:{port}"
            ),
            host=host,
            port=port,
        )

        # Register peer locally.
        self.dht.add_peer(
            peer
        )

        message = {
            "type": "PING",
            "node_id": self.node_id_hex,
        }

        self.network.send(
            encode_message(message),
            host,
            port,
        )

        print(
            f"[PING] PING sent "
            f"to {host}:{port}"
        )

    async def _handle_ping(
        self,
        message,
        addr,
    ):
        """
        Handle incoming PING and return PONG.
        """

        sender_id = message.get(
            "node_id"
        )

        # Register sender in DHT.
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

                self.dht.add_peer(
                    peer
                )

            except (
                ValueError,
                TypeError,
            ):

                pass

        response = {
            "type": "PONG",
            "node_id": self.node_id_hex,
        }

        await self.send_message(
            self._peer_from_address(addr),
            response,
        )

        print(
            f"[PING] PONG sent "
            f"to {addr[0]}:{addr[1]}"
        )

    def handle_pong(
        self,
        message,
        addr,
    ):
        """
        Process PONG received from another peer.
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

                self.dht.add_peer(
                    peer
                )

            except (
                ValueError,
                TypeError,
            ):

                pass

        print(
            f"[PING] PONG received "
            f"from {addr[0]}:{addr[1]}"
        )

    # ============================================================
    # TASK SENDING
    # ============================================================

    def send_task(
        self,
        function,
        args=(),
        kwargs=None,
        host=None,
        port=None,
    ):
        """
        Serialize and send a Python function to
        another MeshWeaver node.
        """

        if not self.running:

            raise RuntimeError(
                "Mesh node is not running."
            )

        if host is None:

            raise ValueError(
                "Task destination host is required."
            )

        if port is None:

            raise ValueError(
                "Task destination port is required."
            )

        if kwargs is None:
            kwargs = {}

        if not isinstance(args, tuple):
            args = tuple(args)

        # --------------------------------------------------------
        # Generate unique task ID
        # --------------------------------------------------------

        task_id = str(
            uuid.uuid4()
        )

        # --------------------------------------------------------
        # Serialize function + arguments
        #
        # serializer.py produces:
        #
        # {
        #     "function": function,
        #     "args": args,
        #     "kwargs": kwargs
        # }
        # --------------------------------------------------------

        serialized = serialize_task(
            function,
            args,
            kwargs,
        )

        # --------------------------------------------------------
        # JSON itself cannot contain bytes.
        #
        # Convert serialized bytes to hexadecimal text.
        # --------------------------------------------------------

        message = {
            "type": "TASK",
            "task_id": task_id,
            "node_id": self.node_id_hex,
            "task": serialized.hex(),
        }

        # --------------------------------------------------------
        # Destination peer
        # --------------------------------------------------------

        peer = Peer(
            node_id=generate_node_id(
                f"{host}:{port}"
            ),
            host=host,
            port=port,
        )

        # --------------------------------------------------------
        # Send UDP packet
        # --------------------------------------------------------

        self.network.send(
            encode_message(message),
            peer.host,
            peer.port,
        )

        print(
            f"[TASK] {task_id} dispatched "
            f"to {host}:{port}"
        )

        return task_id

    # ============================================================
    # TASK RECEIVING / EXECUTION
    # ============================================================

    async def handle_task(
        self,
        message,
        addr,
    ):
        """
        Receive, deserialize and execute a remote task.
        """

        task_id = message.get(
            "task_id"
        )

        serialized_hex = message.get(
            "task"
        )

        if not task_id:

            print(
                "[TASK] Missing task_id."
            )

            return

        if not serialized_hex:

            print(
                f"[TASK] {task_id}: "
                "Missing serialized task."
            )

            return

        # --------------------------------------------------------
        # Convert hexadecimal string back to bytes
        # --------------------------------------------------------

        try:

            serialized = bytes.fromhex(
                serialized_hex
            )

        except ValueError as exc:

            await self._send_task_result(
                task_id=task_id,
                result=None,
                success=False,
                error=f"Invalid task payload: {exc}",
                addr=addr,
            )

            return

        # --------------------------------------------------------
        # Deserialize and execute
        # --------------------------------------------------------

        try:

            task_data = deserialize_task(
                serialized
            )

            # IMPORTANT:
            #
            # serializer.py returns a DICTIONARY:
            #
            # {
            #     "function": ...,
            #     "args": ...,
            #     "kwargs": ...
            # }
            #
            # Therefore we MUST access dictionary keys.
            # ----------------------------------------------------

            function = task_data[
                "function"
            ]

            args = task_data.get(
                "args",
                (),
            )

            kwargs = task_data.get(
                "kwargs",
                {},
            )

            if kwargs is None:
                kwargs = {}

            if not isinstance(args, tuple):
                args = tuple(args)

            if not isinstance(kwargs, dict):

                raise TypeError(
                    "Task kwargs must be a dictionary."
                )

            print(
                f"[TASK] Executing "
                f"{task_id} "
                f"from "
                f"{addr[0]}:{addr[1]}"
            )

            # ----------------------------------------------------
            # Execute remote function
            # ----------------------------------------------------

            result = function(
                *args,
                **kwargs,
            )

            # ----------------------------------------------------
            # Send successful result
            # ----------------------------------------------------

            await self._send_task_result(
                task_id=task_id,
                result=result,
                success=True,
                error=None,
                addr=addr,
            )

            print(
                f"[TASK] {task_id} "
                f"completed successfully."
            )

        except Exception as exc:

            print(
                f"[TASK] {task_id} failed: "
                f"{exc}"
            )

            # ----------------------------------------------------
            # Send failure result
            # ----------------------------------------------------

            await self._send_task_result(
                task_id=task_id,
                result=None,
                success=False,
                error=str(exc),
                addr=addr,
            )

    # ============================================================
    # TASK RESULT
    # ============================================================

    async def _send_task_result(
        self,
        task_id,
        result,
        success,
        error,
        addr,
    ):
        """
        Send TASK_RESULT to the original sender.
        """

        response = {
            "type": "TASK_RESULT",
            "task_id": task_id,
            "node_id": self.node_id_hex,
            "success": success,
        }

        if success:

            response["result"] = result

        else:

            response["error"] = (
                error or "Unknown task error"
            )

        await self.send_message(
            self._peer_from_address(addr),
            response,
        )

    def handle_task_result(
        self,
        message,
        addr,
    ):
        """
        Handle the result returned by a remote worker.
        """

        task_id = message.get(
            "task_id",
            "unknown",
        )

        if message.get("success"):

            result = message.get(
                "result"
            )

            print(
                f"[TASK RESULT] "
                f"{task_id}: "
                f"{result!s}"
            )

        else:

            error = message.get(
                "error",
                "unknown error",
            )

            print(
                f"[TASK RESULT] "
                f"{task_id} failed: "
                f"{error}"
            )

    # ============================================================
    # DHT FIND_NODE
    # ============================================================

    async def _handle_find_node(
        self,
        message,
        addr,
    ):
        """
        Handle a Kademlia FIND_NODE request.
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
            "type": "FIND_NODE_RESPONSE",
            "request_id": request_id,
            "node_id": self.node_id_hex,
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
        """
        Resolve a pending DHT RPC request.
        """

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

    # ============================================================
    # GOSSIP
    # ============================================================

    def _handle_gossip(
        self,
        message,
        addr,
    ):
        """
        Process CPU/RAM gossip received from another peer.
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

                self.dht.add_peer(
                    peer
                )

            except (
                ValueError,
                TypeError,
            ):

                pass

        self.gossip.handle_gossip(
            message,
            addr,
        )

    # ============================================================
    # DHT RPC
    # ============================================================

    async def _send_rpc(
        self,
        peer: Peer,
        rpc_type: str,
        payload: dict,
    ) -> dict:
        """
        Send a DHT RPC and wait for its response.
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

    # ============================================================
    # GENERIC MESSAGE SENDING
    # ============================================================

    async def send_message(
        self,
        peer: Peer,
        message: dict,
    ):
        """
        Encode and send a protocol message.
        """

        self.network.send(
            encode_message(message),
            peer.host,
            peer.port,
        )

    # ============================================================
    # GOSSIP HELPERS
    # ============================================================

    def get_gossip_peers(
        self,
    ) -> list[Peer]:
        """
        Return known peers for gossip.
        """

        return self.dht.known_peers()

    def get_peer_metrics(self):
        """
        Return remote peer metrics.
        """

        return self.gossip.get_peer_metrics()

    # ============================================================
    # PEER HELPERS
    # ============================================================

    def _peer_from_address(
        self,
        addr,
    ) -> Peer:
        """
        Resolve a Peer from a network address.
        """

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

    # ============================================================
    # BOOTSTRAP
    # ============================================================

    async def bootstrap(
        self,
        host: str,
        port: int,
    ):
        """
        Join the mesh through a bootstrap peer.
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
        Display known DHT peers.
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