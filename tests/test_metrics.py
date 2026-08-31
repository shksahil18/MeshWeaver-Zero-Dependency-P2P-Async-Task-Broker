from meshweaver.metrics import (
    SystemMetrics,
)


def test_metrics_snapshot():

    metrics = SystemMetrics()

    snapshot = metrics.snapshot()

    assert "cpu_percent" in snapshot
    assert "memory_percent" in snapshot

    assert 0 <= snapshot["cpu_percent"] <= 100
    assert 0 <= snapshot["memory_percent"] <= 100