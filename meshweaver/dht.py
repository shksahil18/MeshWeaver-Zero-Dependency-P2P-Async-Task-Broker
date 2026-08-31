import hashlib
from dataclasses import dataclass
from typing import Iterable


KEY_BITS = 160
BUCKET_COUNT = KEY_BITS
BUCKET_SIZE = 20


def generate_node_id(address: str) -> int:
    """
    Generate a deterministic 160-bit node ID from an address.
    """

    digest = hashlib.sha1(
        address.encode("utf-8")
    ).digest()

    return int.from_bytes(
        digest,
        byteorder="big",
    )


def xor_distance(left: int, right: int) -> int:
    """
    Kademlia XOR distance.
    """

    return left ^ right


def bucket_index(
    local_id: int,
    remote_id: int,
) -> int | None:
    """
    Return the Kademlia bucket index for a remote node.
    """

    distance = xor_distance(
        local_id,
        remote_id,
    )

    if distance == 0:
        return None

    return distance.bit_length() - 1


@dataclass(frozen=True)
class Peer:
    """
    Represents a known MeshWeaver peer.
    """

    node_id: int
    host: str
    port: int

    @property
    def node_id_hex(self) -> str:
        return f"{self.node_id:040x}"

    @property
    def address(self) -> tuple[str, int]:
        return self.host, self.port


class RoutingTable:
    """
    Lightweight Kademlia routing table.

    Maintains 160 XOR-distance buckets.
    """

    def __init__(
        self,
        local_node_id: int,
        bucket_size: int = BUCKET_SIZE,
    ):
        self.local_node_id = local_node_id
        self.bucket_size = bucket_size

        self.buckets: list[list[Peer]] = [
            []
            for _ in range(BUCKET_COUNT)
        ]

    def add_peer(self, peer: Peer) -> bool:
        """
        Add a peer to the appropriate Kademlia bucket.

        Returns True when the routing table changed.
        """

        if peer.node_id == self.local_node_id:
            return False

        index = bucket_index(
            self.local_node_id,
            peer.node_id,
        )

        if index is None:
            return False

        bucket = self.buckets[index]

        for existing in bucket:

            if existing.node_id == peer.node_id:

                bucket.remove(existing)
                bucket.append(peer)

                return True

        if len(bucket) >= self.bucket_size:
            return False

        bucket.append(peer)

        return True

    def remove_peer(self, node_id: int):
        """
        Remove a peer from the routing table.
        """

        for bucket in self.buckets:

            bucket[:] = [
                peer
                for peer in bucket
                if peer.node_id != node_id
            ]

    def all_peers(self) -> list[Peer]:
        """
        Return all known peers.
        """

        result = []

        for bucket in self.buckets:
            result.extend(bucket)

        return result

    def closest_peers(
        self,
        target_id: int,
        count: int = 20,
    ) -> list[Peer]:
        """
        Return peers closest to target using XOR distance.
        """

        peers = self.all_peers()

        peers.sort(
            key=lambda peer: xor_distance(
                peer.node_id,
                target_id,
            )
        )

        return peers[:count]

    def contains(self, node_id: int) -> bool:
        return any(
            peer.node_id == node_id
            for peer in self.all_peers()
        )

    def __len__(self):
        return len(self.all_peers())


class KademliaNode:
    """
    Lightweight Kademlia node discovery engine.

    The node delegates network requests to callbacks supplied
    by MeshNode.
    """

    def __init__(
        self,
        local_peer: Peer,
        send_rpc,
    ):
        self.local_peer = local_peer

        self.routing_table = RoutingTable(
            local_node_id=local_peer.node_id
        )

        self.send_rpc = send_rpc

    def add_peer(self, peer: Peer) -> bool:
        return self.routing_table.add_peer(peer)

    def remove_peer(self, node_id: int):
        self.routing_table.remove_peer(node_id)

    def known_peers(self) -> list[Peer]:
        return self.routing_table.all_peers()

    async def bootstrap(self, bootstrap_peer: Peer):
        """
        Join an existing mesh through one known bootstrap peer.
        """

        if (
            bootstrap_peer.node_id
            == self.local_peer.node_id
        ):
            return

        self.add_peer(bootstrap_peer)

        print(
            f"[DHT] Bootstrapping through "
            f"{bootstrap_peer.host}:{bootstrap_peer.port}"
        )

        try:

            response = await self.send_rpc(
                bootstrap_peer,
                "FIND_NODE",
                {
                    "target_id":
                        self.local_peer.node_id_hex
                },
            )

            peers = response.get(
                "peers",
                [],
            )

            self._add_peer_records(peers)

            print(
                f"[DHT] Bootstrap discovered "
                f"{len(peers)} peer(s)"
            )

        except TimeoutError:

            print(
                "[DHT] Bootstrap peer did not respond"
            )

        await self.iterative_find_node(
            self.local_peer.node_id
        )

    async def iterative_find_node(
        self,
        target_id: int,
        alpha: int = 3,
        max_rounds: int = 5,
    ) -> list[Peer]:
        """
        Lightweight iterative Kademlia lookup.

        Queries up to alpha closest peers per round.
        """

        shortlist = self.routing_table.closest_peers(
            target_id,
            count=BUCKET_SIZE,
        )

        queried: set[int] = set()

        for _ in range(max_rounds):

            candidates = [
                peer
                for peer in shortlist
                if peer.node_id not in queried
            ]

            candidates = sorted(
                candidates,
                key=lambda peer: xor_distance(
                    peer.node_id,
                    target_id,
                ),
            )[:alpha]

            if not candidates:
                break

            changed = False

            for peer in candidates:

                queried.add(peer.node_id)

                try:

                    response = await self.send_rpc(
                        peer,
                        "FIND_NODE",
                        {
                            "target_id":
                                f"{target_id:040x}"
                        },
                    )

                    remote_peers = response.get(
                        "peers",
                        [],
                    )

                    before = len(shortlist)

                    self._add_peer_records(
                        remote_peers
                    )

                    shortlist.extend(
                        self._peer_from_record(
                            record
                        )
                        for record in remote_peers
                        if self._peer_from_record(
                            record
                        ) is not None
                    )

                    unique = {
                        peer.node_id: peer
                        for peer in shortlist
                        if peer is not None
                    }

                    shortlist = sorted(
                        unique.values(),
                        key=lambda peer: xor_distance(
                            peer.node_id,
                            target_id,
                        ),
                    )[:BUCKET_SIZE]

                    if len(shortlist) > before:
                        changed = True

                except TimeoutError:

                    self.remove_peer(
                        peer.node_id
                    )

            if not changed:
                break

        return shortlist

    def handle_find_node(
        self,
        target_id: int,
    ) -> list[Peer]:
        """
        Return the closest known peers to a target.
        """

        return self.routing_table.closest_peers(
            target_id,
            count=BUCKET_SIZE,
        )

    def _add_peer_records(
        self,
        records: Iterable[dict],
    ):
        for record in records:

            peer = self._peer_from_record(
                record
            )

            if peer is not None:
                self.add_peer(peer)

    @staticmethod
    def _peer_from_record(
        record: dict | None,
    ) -> Peer | None:

        if not record:
            return None

        try:

            node_id = int(
                record["node_id"],
                16,
            )

            return Peer(
                node_id=node_id,
                host=record["host"],
                port=int(record["port"]),
            )

        except (
            KeyError,
            ValueError,
            TypeError,
        ):
            return None

    @staticmethod
    def peer_to_record(peer: Peer) -> dict:
        return {
            "node_id": peer.node_id_hex,
            "host": peer.host,
            "port": peer.port,
        }