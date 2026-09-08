"""
MeshWeaver – peer node.

Weeks 1-4 integrated:
    Week 1  Async UDP networking + task serialization
    Week 2  Kademlia DHT + gossip CPU/RAM exchange
    Week 3  Least-loaded task routing + heartbeat fault tolerance
    Week 4  HMAC-SHA256 message signing + verification
"""

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

from meshweaver.heartbeat import (
    HeartbeatMonitor,
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

from meshweaver.router import (
    NoAvailableNodeError,
    TaskRouter,
)

from meshweaver.tasks.serializer import (
    serialize_task,
    deserialize_task,
)

from meshweaver.tasks.tracker import (
    TaskStatus,
    TaskTracker,
    TaskFailedError,
)


RPC_TIMEOUT = 3
TASK_TIMEOUT = 30     # seconds before a dispatched task is retried


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
        - Least-loaded task routing            (Week 3)
        - Heartbeat-based fault tolerance      (Week 3)
        - HMAC-SHA256 message signing          (Week 4)
    """

    def __init__(
        self,
        host: str,
        port: int,
        sign_key: bytes | None = None,
    ):
        self.host = host
        self.port = port

        # --------------------------------------------------------
        # Week 4 — optional HMAC signing key
        # --------------------------------------------------------
        self._sign_key = sign_key

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
        # Week 3 — Heartbeat monitor
        # --------------------------------------------------------

        self.heartbeat = HeartbeatMonitor(
            node=self,
            interval=2.0,
            timeout=6.0,
            on_peer_offline=self._on_peer_offline,
            on_peer_online=self._on_peer_online,
        )

        # --------------------------------------------------------
        # Week 3 — Task router
        # --------------------------------------------------------

        self.router = TaskRouter(node=self)

        # --------------------------------------------------------
        # Week 3 — Task tracker (ledger)
        # --------------------------------------------------------

        self.tracker = TaskTracker(max_attempts=3)

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
        """Start the MeshWeaver node."""

        await self.network.start()

        self.running = True

        await self.gossip.start()
        await self.heartbeat.start()

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
        print(
            "Heartbeat : every 2 seconds  timeout=6 s"
        )
        if self._sign_key:
            print(
                "Security  : HMAC-SHA256 signing ENABLED"
            )
        print("=" * 65)
        print()

    async def stop(self):
        """Stop the MeshWeaver node."""

        self.running = False

        await self.gossip.stop()
        await self.heartbeat.stop()

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

        # -----------------------------------------------------------
        # Week 3 — Mark sender alive (every inbound message counts).
        # -----------------------------------------------------------
        self.heartbeat.mark_alive(addr)

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

        # Only print non-heartbeat pings to reduce noise.
        if not message.get("heartbeat"):
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

        if not message.get("heartbeat"):
            print(
                f"[PING] PONG received "
                f"from {addr[0]}:{addr[1]}"
            )

    # ============================================================
    # WEEK 3 — ROUTING-AWARE TASK SUBMISSION
    # ============================================================

    async def route_task(
        self,
        function,
        args=(),
        kwargs=None,
        max_attempts: int = 3,
    ) -> str:
        """
        Serialize, route to the lowest-CPU peer, and submit a task.

        Returns the task_id. Use ``wait_for_result(task_id)`` to await
        the outcome.

        Fault tolerance: if a peer goes offline before a result arrives
        the heartbeat callback will automatically re-dispatch the task
        to the next best available peer.
        """

        if not self.running:
            raise RuntimeError("Mesh node is not running.")

        if kwargs is None:
            kwargs = {}

        if not isinstance(args, tuple):
            args = tuple(args)

        # Serialize the function once; reuse the payload on re-routes.
        payload = serialize_task(function, args, kwargs)

        task_id = str(uuid.uuid4())

        record = self.tracker.create(
            function=function,
            args=args,
            kwargs=kwargs,
            payload=payload,
            max_attempts=max_attempts,
            task_id=task_id,
        )

        peers = self.dht.known_peers()

        # Create a router-level route record.
        self.router.create_route(task_id, payload, peers)

        await self._dispatch_task(record)

        return task_id

    async def _dispatch_task(self, record):
        """
        Pick the best peer and send the task.  May raise
        NoAvailableNodeError if no online peer exists.
        """

        try:

            peer = self.router.select_node(
                exclude=record.tried
            )

        except NoAvailableNodeError:

            self.tracker.mark_failed(
                record.task_id,
                "No available node for routing.",
                final=True,
            )

            self.router.mark_failed(
                record.task_id,
                None,
                "No available node.",
            )

            print(
                f"[ROUTER] {record.task_id[:8]} — "
                "no available node, task failed."
            )

            return

        print(
            f"[ROUTER] Selected {peer.host}:{peer.port} "
            f"for task {record.task_id[:8]}"
        )

        self.tracker.mark_dispatched(
            record.task_id,
            peer.address,
            node_id_hex=peer.node_id_hex,
        )

        self.router.mark_dispatched(record.task_id, peer)

        # Build and send the TASK message.
        message = {
            "type": "TASK",
            "task_id": record.task_id,
            "node_id": self.node_id_hex,
            "task": (record.payload or
                     serialize_task(
                         record.function,
                         record.args,
                         record.kwargs,
                     )).hex(),
        }

        # Week 4 — sign if a key is configured.
        if self._sign_key:
            from meshweaver.security import sign_message
            message = sign_message(self._sign_key, message)

        self.network.send(
            encode_message(message),
            peer.host,
            peer.port,
        )

        print(
            f"[TASK] {record.task_id} routed "
            f"to {peer.host}:{peer.port}"
        )

    async def wait_for_result(
        self,
        task_id: str,
        timeout: float = 60.0,
    ):
        """
        Await the final outcome of a routed task.

        Returns the result on success.
        Raises TaskFailedError on permanent failure.
        Raises asyncio.TimeoutError when ``timeout`` expires.
        """

        record = self.tracker.get(task_id)

        if record is None:
            raise KeyError(f"Unknown task_id: {task_id}")

        await asyncio.wait_for(
            record.done.wait(),
            timeout=timeout,
        )

        if record.status is TaskStatus.COMPLETED:
            return record.result

        raise TaskFailedError(
            record.error or "Task failed."
        )

    # ============================================================
    # TASK SENDING (original direct-address API — Week 1/2 compat)
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
        Serialize and send a Python function to another MeshWeaver node.

        If host/port are omitted, the router selects the best available peer.
        """

        if not self.running:

            raise RuntimeError(
                "Mesh node is not running."
            )

        if kwargs is None:
            kwargs = {}

        if not isinstance(args, tuple):
            args = tuple(args)

        task_id = str(
            uuid.uuid4()
        )

        serialized = serialize_task(
            function,
            args,
            kwargs,
        )

        # -------------------------------------------------------------------
        # Auto-route when no explicit destination is given.
        # -------------------------------------------------------------------

        if host is None or port is None:

            peers = self.dht.known_peers()

            if not peers:
                raise NoAvailableNodeError(
                    "No known peers available for routing."
                )

            try:
                peer = self.router.select_worker(peers)
                host = peer.host
                port = peer.port
                print(
                    f"[ROUTER] Auto-selected {host}:{port} "
                    f"for task {task_id[:8]}"
                )
            except NoAvailableNodeError:
                raise

        if host is None:

            raise ValueError(
                "Task destination host is required."
            )

        if port is None:

            raise ValueError(
                "Task destination port is required."
            )

        message = {
            "type": "TASK",
            "task_id": task_id,
            "node_id": self.node_id_hex,
            "task": serialized.hex(),
        }

        # Week 4 — sign if a key is configured.
        if self._sign_key:
            from meshweaver.security import sign_message
            message = sign_message(self._sign_key, message)

        # Create a tracker record for this task.
        record = self.tracker.create(
            function=function,
            args=args,
            kwargs=kwargs,
            payload=serialized,
            task_id=task_id,
        )

        self.tracker.mark_dispatched(
            task_id,
            (host, port),
        )

        # Create a router route record.
        peer_obj = Peer(
            node_id=generate_node_id(f"{host}:{port}"),
            host=host,
            port=port,
        )
        self.router.create_route(task_id, serialized, [peer_obj])
        self.router.mark_dispatched(task_id, peer_obj)

        peer_dest = Peer(
            node_id=generate_node_id(
                f"{host}:{port}"
            ),
            host=host,
            port=port,
        )

        self.network.send(
            encode_message(message),
            peer_dest.host,
            peer_dest.port,
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

        # Week 4 — verify signature when key is configured.
        if self._sign_key:
            from meshweaver.security import verify_message, SignatureError
            try:
                verify_message(self._sign_key, message)
            except SignatureError as exc:
                print(
                    f"[SECURITY] Rejected task from "
                    f"{addr}: {exc}"
                )
                return

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

            result = function(
                *args,
                **kwargs,
            )

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

            # Update tracker and router.
            record = self.tracker.get(task_id)
            if record and record.status is TaskStatus.DISPATCHED:
                self.tracker.mark_completed(task_id, result)

            route = self.router.get_route(task_id)
            if route and route.status == "running":
                self.router.mark_completed(task_id, result)

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

            record = self.tracker.get(task_id)

            if record and record.status is TaskStatus.DISPATCHED:

                if self.tracker.can_retry(task_id):

                    self.tracker.mark_failed(
                        task_id,
                        error,
                        final=False,
                    )

                    route = self.router.get_route(task_id)
                    if route:
                        self.router.mark_failed(
                            task_id,
                            route.current_peer,
                            error,
                        )

                    asyncio.create_task(
                        self._dispatch_task(record)
                    )

                else:

                    self.tracker.mark_failed(
                        task_id,
                        error,
                        final=True,
                    )

                    route = self.router.get_route(task_id)
                    if route:
                        self.router.mark_failed(
                            task_id,
                            route.current_peer,
                            error,
                        )

    # ============================================================
    # WEEK 3 — FAULT TOLERANCE CALLBACKS
    # ============================================================

    def _on_peer_offline(self, address):
        """
        Heartbeat callback: a peer went offline.

        Mark its DISPATCHED tasks as failed and re-route them.
        """

        affected = self.tracker.dispatched_to(address)

        if not affected:
            return

        print(
            f"[FAULT] {address[0]}:{address[1]} offline — "
            f"{len(affected)} task(s) need re-routing."
        )

        for record in affected:

            self.tracker.mark_failed(
                record.task_id,
                f"Node {address[0]}:{address[1]} went offline.",
                final=False,
            )

            route = self.router.get_route(record.task_id)
            if route:
                self.router.mark_failed(
                    record.task_id,
                    route.current_peer,
                    "node offline",
                )

            if self.tracker.can_retry(record.task_id):

                asyncio.create_task(
                    self._dispatch_task(record)
                )

            else:

                self.tracker.mark_failed(
                    record.task_id,
                    "Max retries exceeded after node failure.",
                    final=True,
                )

    def _on_peer_online(self, address):
        """
        Heartbeat callback: a peer came back online.
        """

        print(
            f"[FAULT] {address[0]}:{address[1]} is back online."
        )

    # ============================================================
    # WEEK 3 — send_to_address (required by HeartbeatMonitor)
    # ============================================================

    async def send_to_address(
        self,
        address: tuple,
        message: dict,
    ):
        """
        Encode and send a protocol message to a raw (host, port) tuple.

        Used by HeartbeatMonitor to send lightweight PING probes.
        """

        self.network.send(
            encode_message(message),
            address[0],
            address[1],
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
        Return remote peer metrics (from gossip engine).
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