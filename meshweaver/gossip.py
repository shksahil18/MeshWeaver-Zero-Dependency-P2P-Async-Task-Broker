import asyncio
from datetime import datetime, timezone


GOSSIP_INTERVAL = 5


class GossipEngine:
    """
    Periodically broadcasts local CPU/RAM metrics
    to known DHT neighbors.
    """

    def __init__(
        self,
        node,
        interval: int = GOSSIP_INTERVAL,
    ):
        self.node = node
        self.interval = interval

        self.running = False
        self.task = None

        self.peer_metrics: dict[int, dict] = {}

    async def start(self):
        """
        Start background gossip loop.
        """

        if self.running:
            return

        self.running = True

        self.task = asyncio.create_task(
            self._gossip_loop()
        )

        print(
            f"[GOSSIP] Started "
            f"(interval={self.interval}s)"
        )

    async def stop(self):
        """
        Stop background gossip loop.
        """

        self.running = False

        if self.task is not None:

            self.task.cancel()

            try:
                await self.task

            except asyncio.CancelledError:
                pass

            self.task = None

        print("[GOSSIP] Stopped")

    async def _gossip_loop(self):
        """
        Broadcast metrics every configured interval.
        """

        while self.running:

            try:

                await self.broadcast_metrics()

                await asyncio.sleep(
                    self.interval
                )

            except asyncio.CancelledError:
                raise

            except Exception as exc:

                print(
                    f"[GOSSIP] Error: {exc}"
                )

                await asyncio.sleep(
                    self.interval
                )

    async def broadcast_metrics(self):
        """
        Send current CPU/RAM usage to known peers.
        """

        metrics = self.node.metrics.snapshot()

        peers = self.node.get_gossip_peers()

        if not peers:
            print(
                "[GOSSIP] No known neighbors"
            )
            return

        for peer in peers:

            try:

                await self.node.send_message(
                    peer,
                    {
                        "type": "GOSSIP",
                        "node_id":
                            self.node.node_id_hex,
                        "cpu_percent":
                            metrics["cpu_percent"],
                        "memory_percent":
                            metrics["memory_percent"],
                        "timestamp":
                            datetime.now(
                                timezone.utc
                            ).isoformat(),
                    },
                )

                print(
                    f"[GOSSIP] Sent metrics to "
                    f"{peer.host}:{peer.port} "
                    f"| CPU="
                    f"{metrics['cpu_percent']:.2f}% "
                    f"| RAM="
                    f"{metrics['memory_percent']:.2f}%"
                )

            except Exception as exc:

                print(
                    f"[GOSSIP] Failed to send to "
                    f"{peer.host}:{peer.port}: "
                    f"{exc}"
                )

    def handle_gossip(
        self,
        message: dict,
        sender_address,
    ):
        """
        Store metrics received from another node.
        """

        try:

            node_id = int(
                message["node_id"],
                16,
            )

            cpu = float(
                message["cpu_percent"]
            )

            memory = float(
                message["memory_percent"]
            )

            timestamp = message.get(
                "timestamp"
            )

            self.peer_metrics[node_id] = {
                "cpu_percent": cpu,
                "memory_percent": memory,
                "timestamp": timestamp,
                "address": sender_address,
            }

            print(
                f"[GOSSIP] Received from "
                f"{node_id:040x} "
                f"| CPU={cpu:.2f}% "
                f"| RAM={memory:.2f}%"
            )

        except (
            KeyError,
            ValueError,
            TypeError,
        ) as exc:

            print(
                f"[GOSSIP] Invalid metrics: "
                f"{exc}"
            )

    def get_peer_metrics(self) -> dict:
        """
        Return currently known remote metrics.
        """

        return dict(self.peer_metrics)