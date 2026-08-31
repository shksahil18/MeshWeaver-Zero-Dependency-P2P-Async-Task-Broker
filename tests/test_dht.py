from meshweaver.dht import (
    Peer,
    RoutingTable,
    bucket_index,
    generate_node_id,
    xor_distance,
)


def test_node_id_is_160_bit():

    node_id = generate_node_id(
        "127.0.0.1:9001"
    )

    assert 0 <= node_id < (1 << 160)


def test_xor_distance():

    assert xor_distance(10, 12) == 6


def test_bucket_index():

    local = 0
    remote = 1

    assert bucket_index(
        local,
        remote,
    ) == 0


def test_routing_table_adds_peer():

    local_id = generate_node_id(
        "127.0.0.1:9001"
    )

    remote_id = generate_node_id(
        "127.0.0.1:9002"
    )

    table = RoutingTable(local_id)

    peer = Peer(
        node_id=remote_id,
        host="127.0.0.1",
        port=9002,
    )

    assert table.add_peer(peer)

    assert len(table) == 1


def test_closest_peer():

    local_id = 0

    table = RoutingTable(local_id)

    peer_a = Peer(
        node_id=10,
        host="127.0.0.1",
        port=9001,
    )

    peer_b = Peer(
        node_id=20,
        host="127.0.0.1",
        port=9002,
    )

    table.add_peer(peer_a)
    table.add_peer(peer_b)

    closest = table.closest_peers(
        target_id=11,
        count=1,
    )

    assert closest[0].node_id == 10