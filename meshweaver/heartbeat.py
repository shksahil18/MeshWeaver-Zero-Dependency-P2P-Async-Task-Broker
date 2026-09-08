"""
Week 3 - Fault Tolerance: heartbeat monitoring.

Every ``interval`` seconds the HeartbeatMonitor sends a lightweight
PING (flagged ``"heartbeat": true``) to every peer in the DHT.

Every message that arrives from a peer - PONG, GOSSIP, TASK_RESULT,
anything - refreshes that peer's "last seen" clock through
``mark_alive()``.

If a peer has not been heard from for ``timeout`` seconds it is
declared OFFLINE and ``on_peer_offline(address)`` is fired. MeshNode
uses that callback to mark the tasks running on that node as FAILED
and to re-route them somewhere else.

Peers are tracked by network address (host, port) rather than node
id, because a crashed *process* is what disappears - and the same
address may be known under two ids (a deterministic bootstrap id and
the node's real random id).

Timeline example (interval=2s, timeout=6s):

    t=0   PING ->  peer            last_seen = 0 (PONG)
    t=2   PING ->  peer (crashed)  no reply
    t=4   PING ->  peer            no reply
    t=6   PING ->  peer            no reply
    t=6+  now - last_seen > 6s     => OFFLINE
"""

import asyncio
import time


HEARTBEAT_INTERVAL = 2.0
HEARTBEAT_TIMEOUT = 6.0


class HeartbeatMonitor:
    """
    Detects peers that dropped off the mesh.
    """

    def __init__(
        self,
        node,
        interval: float = HEARTBEAT_INTERVAL,
        timeout: float = HEARTBEAT_TIMEOUT,
        on_peer_offline=None,
        on_peer_online=None,
    ):
        if timeout <= interval:

            raise ValueError(
                "Heartbeat timeout must be larger than the "
                "heartbeat interval."
            )

        self.node = node
        self.interval = interval
        self.timeout = timeout

        self.on_peer_offline = on_peer_offline
        self.on_peer_online = on_peer_online

        # (host, port) -> monotonic time of the last message received
        self.last_seen: dict[tuple[str, int], float] = {}

        # Addresses currently considered dead.
        self.offline: set[tuple[str, int]] = set()

        self.running = False
        self.task = None

    # ------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------

    async def start(self):

        if self.running:
            return

        self.running = True

        self.task = asyncio.create_task(
            self._heartbeat_loop()
        )

        print(
            f"[HEARTBEAT] Started "
            f"(interval={self.interval}s, "
            f"timeout={self.timeout}s)"
        )

    async def stop(self):

        self.running = False

        if self.task is not None:

            self.task.cancel()

            try:
                await self.task

            except asyncio.CancelledError:
                pass

            self.task = None

        print("[HEARTBEAT] Stopped")

    # ------------------------------------------------------------
    # Liveness bookkeeping
    # ------------------------------------------------------------

    def mark_alive(self, address):
        """
        Record that a message just arrived from ``address``.

        Called by MeshNode for EVERY inbound message, so gossip
        and task results count as heartbeats too.
        """

        address = self._normalize(address)

        self.last_seen[address] = time.monotonic()

        if address in self.offline:

            self.offline.discard(address)

            print(
                f"[HEARTBEAT] {address[0]}:{address[1]} "
                f"is back ONLINE"
            )

            if self.on_peer_online is not None:
                self.on_peer_online(address)

    def is_online(self, address) -> bool:

        return self._normalize(address) not in self.offline

    def seconds_since_seen(self, address) -> float | None:

        seen = self.last_seen.get(
            self._normalize(address)
        )

        if seen is None:
            return None

        return time.monotonic() - seen

    def status(self) -> dict:
        """
        Snapshot of every tracked address.

        {
            (host, port): {
                "status": "ONLINE" | "OFFLINE",
                "last_seen_ago": float | None,
            }
        }
        """

        addresses = set(self._peer_addresses()) | self.offline

        return {
            address: {
                "status": (
                    "OFFLINE"
                    if address in self.offline
                    else "ONLINE"
                ),
                "last_seen_ago": self.seconds_since_seen(
                    address
                ),
            }
            for address in sorted(addresses)
        }

    # ------------------------------------------------------------
    # Heartbeat loop
    # ------------------------------------------------------------

    async def _heartbeat_loop(self):

        while self.running:

            try:
                await self.tick()

            except asyncio.CancelledError:
                raise

            except Exception as exc:

                print(
                    f"[HEARTBEAT] Error: {exc}"
                )

            await asyncio.sleep(self.interval)

    async def tick(self):
        """
        One heartbeat round: PING everyone, then expire the silent.
        """

        now = time.monotonic()

        for address in self._peer_addresses():

            # A peer we have just discovered gets a full timeout
            # window before it can be declared offline.
            self.last_seen.setdefault(address, now)

            await self.node.send_to_address(
                address,
                {
                    "type": "PING",
                    "node_id": self.node.node_id_hex,
                    "heartbeat": True,
                },
            )

        self.check_timeouts(now)

    def check_timeouts(self, now=None) -> list[tuple[str, int]]:
        """
        Declare silent peers OFFLINE. Returns the newly dead ones.

        Separated from ``tick`` so it can be unit-tested with a
        synthetic clock.
        """

        if now is None:
            now = time.monotonic()

        newly_offline = []

        for address in self._peer_addresses():

            if address in self.offline:
                continue

            seen = self.last_seen.get(address)

            if seen is None:
                continue

            if now - seen > self.timeout:

                self.offline.add(address)

                newly_offline.append(address)

        for address in newly_offline:

            silent_for = now - self.last_seen[address]

            print(
                f"[HEARTBEAT] {address[0]}:{address[1]} "
                f"OFFLINE - silent for {silent_for:.1f}s "
                f"(timeout {self.timeout}s)"
            )

            if self.on_peer_offline is not None:
                self.on_peer_offline(address)

        return newly_offline

    # ------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------

    def _peer_addresses(self) -> list[tuple[str, int]]:
        """
        Unique peer addresses currently in the DHT (excluding us).
        """

        own = self._normalize(self.node.peer.address)

        addresses = {
            self._normalize(peer.address)
            for peer in self.node.dht.known_peers()
        }

        addresses.discard(own)

        return sorted(addresses)

    @staticmethod
    def _normalize(address) -> tuple[str, int]:
        return (
            str(address[0]),
            int(address[1]),
        )
