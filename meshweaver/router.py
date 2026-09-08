"""
Week 3 - Task Routing.

The TaskRouter decides WHICH node a submitted task should run on.

Algorithm (least-loaded routing):

    1. Collect the candidate peer list.
    2. Drop excluded node IDs and offline addresses.
    3. Attach the most recent CPU/RAM metrics received via GOSSIP.
       Metrics older than ``METRICS_MAX_AGE`` seconds are "stale".
    4. Sort candidates by:
           a) fresh metrics first, stale/unknown last
           b) lowest CPU percent
           c) lowest RAM percent      (tie-breaker)
           d) address                 (deterministic final tie-breaker)
    5. The first candidate wins.

``select_worker(peers, excluded)``
    Lightweight selector operating on an explicit list of Peer objects.
    Used by unit tests and by MeshNode when it already has a peer list.

``select_node(exclude)``
    Full selector that queries the DHT routing table, checks
    HeartbeatMonitor liveness, and applies gossip metrics.
    Used by MeshNode when routing an incoming task submission.
"""

import math
import time
from dataclasses import dataclass, field


# A gossip metric is fresh when younger than this many seconds.
# Gossip runs every 5 s → 3 missed rounds ≈ stale.
METRICS_MAX_AGE = 15.0


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------


class NoAvailableNodeError(RuntimeError):
    """Raised when no eligible node exists to run a task."""


# ---------------------------------------------------------------------------
# Route record
# ---------------------------------------------------------------------------


@dataclass
class TaskRoute:
    """
    Everything the router knows about one submitted task.

    status transitions:
        pending → running → completed
                         └→ failed (→ running again on re-route)
    """

    task_id: str
    payload: bytes
    candidates: list  # original peer list passed to create_route

    status: str = "pending"          # pending / running / failed / completed
    current_peer: object = None      # Peer currently assigned
    result: object = None

    failure_history: list = field(default_factory=list)

    def record_failure(self, peer, reason: str):
        self.failure_history.append(
            {
                "peer": peer,
                "reason": reason,
                "at": time.monotonic(),
            }
        )

    @property
    def failed_peers(self) -> set:
        """Node IDs of every peer that has already failed this task."""
        return {
            entry["peer"].node_id
            for entry in self.failure_history
            if entry["peer"] is not None
        }


# ---------------------------------------------------------------------------
# Main router
# ---------------------------------------------------------------------------


class TaskRouter:
    """
    Least-loaded task router.

    The ``node`` argument must expose:

        node.get_peer_metrics()
            → dict[int, dict]   keyed by node_id (int)
              each entry may contain:
                  "cpu_percent"    float
                  "memory_percent" float
                  "received_at"    float  (time.monotonic())
                  "address"        (host, port)

        node.dht.known_peers()   → list[Peer]        (full-DHT mode)
        node.peer.address        → (host, port)       (full-DHT mode)
        node.heartbeat.is_online(address) → bool      (full-DHT mode)
    """

    def __init__(
        self,
        node,
        metrics_max_age: float = METRICS_MAX_AGE,
    ):
        self.node = node
        self.metrics_max_age = metrics_max_age
        self._routes: dict[str, TaskRoute] = {}

    # -----------------------------------------------------------------------
    # Simple peer selection (test-friendly & fast)
    # -----------------------------------------------------------------------

    def select_worker(
        self,
        peers,
        excluded: set | None = None,
    ):
        """
        Return the peer with the lowest reported CPU load.

        Parameters
        ----------
        peers    : list of Peer-like objects (must have .node_id attribute)
        excluded : set of node_id (int) values to skip

        Returns the winning Peer. Raises NoAvailableNodeError when nothing
        is left after filtering.
        """

        if not peers:
            raise NoAvailableNodeError(
                "No peers supplied to select_worker."
            )

        if excluded is None:
            excluded = set()

        eligible = [
            p for p in peers
            if p.node_id not in excluded
        ]

        if not eligible:
            raise NoAvailableNodeError(
                "All supplied peers are excluded."
            )

        metrics = self.node.get_peer_metrics()

        def sort_key(peer):
            entry = metrics.get(peer.node_id, {})
            cpu = float(entry.get("cpu_percent", math.inf))
            ram = float(entry.get("memory_percent", math.inf))
            return (
                0 if cpu is not math.inf else 1,
                cpu,
                ram,
                peer.host if hasattr(peer, "host") else "",
                peer.port if hasattr(peer, "port") else 0,
            )

        return min(eligible, key=sort_key)

    # -----------------------------------------------------------------------
    # DHT-aware selection (used by MeshNode)
    # -----------------------------------------------------------------------

    def select_node(self, exclude=()):
        """
        Select the best online peer from the DHT routing table.

        ``exclude`` may contain (host, port) tuples or Peer objects.
        Raises NoAvailableNodeError when nothing is available.
        """

        ranked = self.rank_nodes(exclude=exclude)

        if not ranked:
            raise NoAvailableNodeError(
                "No online peer available for task routing."
            )

        return ranked[0]["peer"]

    def rank_nodes(self, exclude=()) -> list[dict]:
        """
        Return all eligible peers sorted best-first.

        Each element is a dict with keys:
            peer, cpu, ram, fresh
        """

        excluded_addrs = set()
        for item in exclude:
            if hasattr(item, "address"):
                excluded_addrs.add(
                    (str(item.address[0]), int(item.address[1]))
                )
            elif hasattr(item, "__iter__"):
                excluded_addrs.add(
                    (str(item[0]), int(item[1]))
                )

        # Always exclude ourselves.
        if hasattr(self.node, "peer"):
            own = self.node.peer.address
            excluded_addrs.add((str(own[0]), int(own[1])))

        metrics_by_addr = self._metrics_by_address()
        now = time.monotonic()
        seen_addrs: set = set()
        candidates = []

        for peer in self.node.dht.known_peers():

            address = (str(peer.host), int(peer.port))

            if address in excluded_addrs:
                continue

            if address in seen_addrs:
                continue

            seen_addrs.add(address)

            # Skip if HeartbeatMonitor says this peer is offline.
            if hasattr(self.node, "heartbeat"):
                if not self.node.heartbeat.is_online(address):
                    continue

            metrics = metrics_by_addr.get(address)

            if metrics is None:
                candidates.append(
                    {
                        "peer": peer,
                        "cpu": math.inf,
                        "ram": math.inf,
                        "fresh": False,
                    }
                )
                continue

            age = now - metrics["received_at"]

            candidates.append(
                {
                    "peer": peer,
                    "cpu": metrics["cpu_percent"],
                    "ram": metrics["memory_percent"],
                    "fresh": age <= self.metrics_max_age,
                }
            )

        candidates.sort(
            key=lambda c: (
                0 if c["fresh"] else 1,
                c["cpu"],
                c["ram"],
                c["peer"].host if hasattr(c["peer"], "host") else "",
                c["peer"].port if hasattr(c["peer"], "port") else 0,
            )
        )

        return candidates

    def format_table(self, exclude=()) -> str:
        """Return the routing table as printable text."""

        ranked = self.rank_nodes(exclude=exclude)

        if not ranked:
            return "[ROUTER] No eligible nodes."

        lines = ["[ROUTER] Routing table (best first):"]

        for index, c in enumerate(ranked, 1):

            marker = "->" if index == 1 else "  "
            peer = c["peer"]
            fresh = "fresh" if c["fresh"] else "stale"

            cpu_str = (
                f"{c['cpu']:5.1f}%"
                if c["cpu"] is not math.inf
                else "   ?  "
            )
            ram_str = (
                f"{c['ram']:5.1f}%"
                if c["ram"] is not math.inf
                else "   ?  "
            )

            lines.append(
                f"  {marker} {index}. "
                f"{peer.host}:{peer.port:<6}  "
                f"CPU={cpu_str}  RAM={ram_str}  [{fresh}]"
            )

        return "\n".join(lines)

    # -----------------------------------------------------------------------
    # Route lifecycle — used by MeshNode for fault-tolerant dispatch
    # -----------------------------------------------------------------------

    def create_route(
        self,
        task_id: str,
        payload: bytes,
        candidates: list,
    ) -> TaskRoute:
        """Register a new task and return its TaskRoute."""

        route = TaskRoute(
            task_id=task_id,
            payload=payload,
            candidates=list(candidates),
        )
        self._routes[task_id] = route
        return route

    def mark_dispatched(
        self,
        task_id: str,
        peer,
    ) -> TaskRoute:
        route = self._require(task_id)
        route.status = "running"
        route.current_peer = peer
        return route

    def mark_failed(
        self,
        task_id: str,
        peer,
        reason: str,
    ) -> TaskRoute:
        route = self._require(task_id)
        route.status = "failed"
        route.record_failure(peer, reason)
        return route

    def mark_completed(
        self,
        task_id: str,
        result=None,
    ) -> TaskRoute:
        route = self._require(task_id)
        route.status = "completed"
        route.result = result
        return route

    def get_route(self, task_id: str) -> TaskRoute | None:
        return self._routes.get(task_id)

    def all_routes(self) -> list[TaskRoute]:
        return list(self._routes.values())

    # -----------------------------------------------------------------------
    # Helpers
    # -----------------------------------------------------------------------

    def _metrics_by_address(self) -> dict:
        """
        Build a {(host, port): metric_dict} mapping from gossip data.

        Gossip stores metrics keyed by node_id (int). Multiple node IDs can
        resolve to the same address (bootstrap ID vs real ID). We keep only
        the freshest entry per address.
        """

        result: dict[tuple, dict] = {}

        for node_id, entry in self.node.get_peer_metrics().items():

            address = entry.get("address")

            if not address:
                continue

            address = (str(address[0]), int(address[1]))
            received_at = float(entry.get("received_at", 0.0))

            existing = result.get(address)

            if existing and existing["received_at"] >= received_at:
                continue

            result[address] = {
                "cpu_percent": float(
                    entry.get("cpu_percent", math.inf)
                ),
                "memory_percent": float(
                    entry.get("memory_percent", math.inf)
                ),
                "received_at": received_at,
                "node_id_hex": f"{node_id:040x}",
            }

        return result

    def _require(self, task_id: str) -> TaskRoute:

        route = self._routes.get(task_id)

        if route is None:
            raise KeyError(f"Unknown task_id: {task_id}")

        return route
