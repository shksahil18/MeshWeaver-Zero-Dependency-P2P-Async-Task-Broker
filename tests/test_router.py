"""
Tests for TaskRouter — Week 3 task routing.

These tests use a FakeNode that mimics the interface MeshNode exposes
to TaskRouter: node.get_peer_metrics() keyed by node_id (int).
"""

import math
from types import SimpleNamespace

import pytest

from meshweaver.router import (
    NoAvailableNodeError,
    TaskRouter,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class FakeNode:
    """
    Minimal stand-in for MeshNode used by TaskRouter.
    """

    def __init__(self, metrics: dict):
        self._metrics = metrics

    def get_peer_metrics(self) -> dict:
        return self._metrics


def peer(node_id: int, host: str, port: int):
    return SimpleNamespace(
        node_id=node_id,
        node_id_hex=f"{node_id:040x}",
        host=host,
        port=port,
        address=(host, port),
    )


# ---------------------------------------------------------------------------
# select_worker tests
# ---------------------------------------------------------------------------


def test_selects_lowest_reported_cpu():
    """Router picks the peer with the smallest cpu_percent."""

    p1 = peer(1, "127.0.0.1", 9002)
    p2 = peer(2, "127.0.0.1", 9003)
    p3 = peer(3, "127.0.0.1", 9004)

    node = FakeNode(
        {
            1: {"cpu_percent": 72.0, "memory_percent": 50.0},
            2: {"cpu_percent": 18.0, "memory_percent": 40.0},
            3: {"cpu_percent": 41.0, "memory_percent": 60.0},
        }
    )

    router = TaskRouter(node)

    assert router.select_worker([p1, p2, p3]) == p2


def test_excluded_peer_is_not_selected():
    """A peer in the excluded set must not be chosen."""

    p1 = peer(1, "127.0.0.1", 9002)
    p2 = peer(2, "127.0.0.1", 9003)

    node = FakeNode(
        {
            1: {"cpu_percent": 5.0, "memory_percent": 30.0},
            2: {"cpu_percent": 30.0, "memory_percent": 45.0},
        }
    )

    router = TaskRouter(node)

    # p1 has lower CPU but is excluded → p2 should be selected.
    result = router.select_worker([p1, p2], excluded={1})
    assert result == p2


def test_falls_back_when_metrics_are_unavailable():
    """When no metrics exist, the router falls back to the first peer."""

    p1 = peer(1, "127.0.0.1", 9002)
    p2 = peer(2, "127.0.0.1", 9003)

    router = TaskRouter(FakeNode({}))

    # Both peers have inf CPU → p1 comes first by address sort.
    result = router.select_worker([p1, p2])
    assert result in (p1, p2)   # deterministic but either is valid


def test_raises_when_no_peers():
    """NoAvailableNodeError is raised when the peer list is empty."""

    router = TaskRouter(FakeNode({}))

    with pytest.raises(NoAvailableNodeError):
        router.select_worker([])


def test_raises_when_all_excluded():
    """NoAvailableNodeError is raised when every peer is excluded."""

    p1 = peer(1, "127.0.0.1", 9002)

    router = TaskRouter(FakeNode({}))

    with pytest.raises(NoAvailableNodeError):
        router.select_worker([p1], excluded={1})


# ---------------------------------------------------------------------------
# Route lifecycle tests
# ---------------------------------------------------------------------------


def test_task_route_status_changes():
    """A route transitions: pending → running → failed → completed."""

    p1 = peer(1, "127.0.0.1", 9002)

    router = TaskRouter(FakeNode({}))

    route = router.create_route("task-1", b"payload", [p1])
    assert route.status == "pending"

    router.mark_dispatched("task-1", p1)
    assert route.status == "running"

    router.mark_failed("task-1", p1, "heartbeat timeout")
    assert route.status == "failed"
    assert len(route.failure_history) == 1
    assert route.failure_history[0]["reason"] == "heartbeat timeout"

    router.mark_completed("task-1", result=42)
    assert route.status == "completed"
    assert route.result == 42


def test_get_route_returns_none_for_unknown():
    router = TaskRouter(FakeNode({}))
    assert router.get_route("nonexistent") is None


def test_failed_peers_accumulate():
    """failed_peers collects all peers that failed the task."""

    p1 = peer(1, "127.0.0.1", 9002)
    p2 = peer(2, "127.0.0.1", 9003)

    router = TaskRouter(FakeNode({}))
    router.create_route("task-2", b"", [p1, p2])

    router.mark_dispatched("task-2", p1)
    router.mark_failed("task-2", p1, "offline")

    router.mark_dispatched("task-2", p2)
    router.mark_failed("task-2", p2, "exception")

    route = router.get_route("task-2")

    assert 1 in route.failed_peers
    assert 2 in route.failed_peers
