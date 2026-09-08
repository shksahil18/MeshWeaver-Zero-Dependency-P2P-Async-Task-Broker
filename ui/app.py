"""Flask UI adapter for MeshWeaver.

The adapter owns only web-dashboard state. It uses the public ``MeshNode``
interface and never changes the broker package itself.
"""

import asyncio
import threading
import time
from datetime import datetime, timezone

from flask import Flask, jsonify, render_template, request

from meshweaver.node import MeshNode
from meshweaver.security import key_from_env


def echo(value="Hello from MeshWeaver"):
    """A safe, built-in task that can be dispatched from the UI."""
    return value


def add(left=0, right=0):
    """A safe arithmetic task that can be dispatched from the UI."""
    return float(left) + float(right)


def multiply(left=1, right=1):
    """A safe arithmetic task that can be dispatched from the UI."""
    return float(left) * float(right)


TASKS = {"echo": echo, "add": add, "multiply": multiply}


class DashboardNode(MeshNode):
    """Observe broker events for the dashboard while retaining node behavior."""

    def __init__(self, host, port, on_event):
        # The optional shared key keeps the web adapter aligned with the
        # Week 4 security feature without adding a new UI control.  When the
        # environment variable is absent, MeshNode keeps its unsigned mode.
        super().__init__(host, port, sign_key=key_from_env())
        self._on_event = on_event
        self._base_peer_offline = self.heartbeat.on_peer_offline
        self._base_peer_online = self.heartbeat.on_peer_online
        self.heartbeat.on_peer_offline = self._handle_peer_offline
        self.heartbeat.on_peer_online = self._handle_peer_online

    def handle_pong(self, message, addr):
        super().handle_pong(message, addr)
        node_id_raw = message.get("node_id", "unknown")
        node_id_str = f"{node_id_raw:040x}" if isinstance(node_id_raw, int) else str(node_id_raw)
        self._on_event("peer", f"Peer is online: {node_id_str}", host=addr[0], port=addr[1], node_id=node_id_str)

    def _handle_peer_offline(self, address):
        if self._base_peer_offline is not None:
            self._base_peer_offline(address)
        self._on_event("error", f"Peer marked offline: {address[0]}:{address[1]}", host=address[0], port=address[1])

    def _handle_peer_online(self, address):
        if self._base_peer_online is not None:
            self._base_peer_online(address)
        self._on_event("peer", f"Peer restored: {address[0]}:{address[1]}", host=address[0], port=address[1])

    def handle_task_result(self, message, addr):
        super().handle_task_result(message, addr)
        is_success = bool(message.get("success"))
        result_val = message.get("result")
        detail = f"Remote task completed: {result_val!s}" if is_success else f"Remote task failed: {message.get('error', 'unknown error')}"
        self._on_event(
            "success" if is_success else "error",
            detail,
            host=addr[0],
            port=addr[1],
            task_id=message.get("task_id"),
            result=str(result_val) if is_success else None,
        )


class NodeController:
    """Run one optional local node on a dedicated asyncio event-loop thread."""

    def __init__(self):
        self._lock = threading.RLock()
        self._node = None
        self._events = []
        self._task_times = {}
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()

    def _run_loop(self):
        asyncio.set_event_loop(self._loop)
        self._loop.run_forever()

    @staticmethod
    def _timestamp():
        return datetime.now(timezone.utc).strftime("%H:%M:%S UTC")

    def _event(self, event_type, message, **extra):
        with self._lock:
            self._events.insert(0, {"type": event_type, "message": message, "time": self._timestamp(), **extra})
            del self._events[20:]

    def _run(self, coroutine):
        return asyncio.run_coroutine_threadsafe(coroutine, self._loop).result(timeout=6)

    async def _start(self, host, port):
        with self._lock:
            if self._node is not None:
                return
            self._node = DashboardNode(host, port, self._event)
            node = self._node
        try:
            await node.start()
        except Exception:
            with self._lock:
                self._node = None
            raise
        self._event("system", f"Local node started at {host}:{port}")

    def start(self, host, port):
        self._run(self._start(host, port))

    async def _stop(self):
        with self._lock:
            node, self._node = self._node, None
        if node is not None:
            await node.stop()
            self._event("system", "Local node stopped")

    def stop(self):
        self._run(self._stop())

    def _node_or_error(self):
        with self._lock:
            if self._node is None:
                raise ValueError("Start the local node before using mesh actions.")
            return self._node

    async def _ping(self, host, port):
        node = self._node_or_error()
        node.ping(host, port)
        self._event("send", f"PING sent to {host}:{port}")

    def ping(self, host, port):
        self._run(self._ping(host, port))

    async def _send_task(self, operation, first, second, host, port, routing="direct"):
        node = self._node_or_error()
        if operation not in TASKS:
            raise ValueError("Unknown task operation.")
        args = (first,) if operation == "echo" else (first, second)
        if routing == "least-loaded":
            task_id = await node.route_task(TASKS[operation], args=args)
            destination = "least-loaded peer"
        else:
            task_id = node.send_task(TASKS[operation], args, None, host, port)
            destination = f"{host}:{port}"
        with self._lock:
            self._task_times[task_id] = self._timestamp()
        self._event("send", f"{operation} task dispatched via {destination}", task_id=task_id)
        return task_id

    def send_task(self, operation, first, second, host, port, routing="direct"):
        return self._run(self._send_task(operation, first, second, host, port, routing))

    def snapshot(self):
        return self._run(self._snapshot())

    async def _snapshot(self):
        with self._lock:
            node = self._node
            events = list(self._events)
            task_times = dict(self._task_times)

        if node is None:
            return {
                "running": False,
                "node_id": None,
                "address": None,
                "peers": [],
                "events": events,
                "tasks": [],
                "security": {"signing_enabled": False},
            }

        # DHT is the source of truth for discovered peers.  This also keeps
        # the UI correct after a heartbeat timeout or a routed retry.
        metrics_by_address = {}
        for entry in node.get_peer_metrics().values():
            address = entry.get("address")
            if not address:
                continue
            address = (str(address[0]), int(address[1]))
            previous = metrics_by_address.get(address)
            if previous is None or entry.get("received_at", 0) > previous.get("received_at", 0):
                metrics_by_address[address] = entry

        peers, seen = [], set()
        for peer in node.dht.known_peers():
            address = (str(peer.host), int(peer.port))
            if address in seen:
                continue
            seen.add(address)
            heartbeat = node.heartbeat.status().get(address, {})
            metric = metrics_by_address.get(address, {})
            received_at = metric.get("received_at")
            peers.append({
                "name": peer.node_id_hex,
                "host": address[0],
                "port": address[1],
                "status": "Online" if heartbeat.get("status", "ONLINE") == "ONLINE" else "Offline",
                "cpu_percent": metric.get("cpu_percent"),
                "memory_percent": metric.get("memory_percent"),
                "last_seen_ago": heartbeat.get("last_seen_ago"),
                "metrics_age": round(time.monotonic() - received_at, 1) if received_at else None,
            })

        tasks = []
        for record in sorted(node.tracker.all(), key=lambda item: item.created_at, reverse=True)[:12]:
            target = "-"
            if record.assigned_to:
                target = f"{record.assigned_to[0]}:{record.assigned_to[1]}"
            tasks.append({
                "id": record.task_id,
                "name": record.name,
                "args": ", ".join(repr(arg) for arg in record.args),
                "target": target,
                "time": task_times.get(record.task_id, "Current session"),
                "status": record.status.value.title(),
                "result": str(record.result) if record.result is not None else None,
                "error": record.error,
                "attempts": record.attempts,
                "max_attempts": record.max_attempts,
                "history": list(record.history),
                "route_status": (
                    node.router.get_route(record.task_id).status
                    if node.router.get_route(record.task_id) is not None
                    else None
                ),
            })

        return {
            "running": bool(node.running),
            "node_id": node.node_id_hex,
            "address": f"{node.host}:{node.port}",
            "peers": peers,
            "events": events,
            "tasks": tasks,
            "task_summary": node.tracker.summary(),
            "local_metrics": node.metrics.snapshot(),
            "security": {"signing_enabled": bool(node._sign_key)},
        }


def request_value(payload, name, default=None):
    value = payload.get(name, default)
    if isinstance(value, str):
        value = value.strip()
    if value in (None, ""):
        raise ValueError(f"{name.replace('_', ' ').capitalize()} is required.")
    return value


def request_port(payload, name="port"):
    try:
        port = int(request_value(payload, name))
    except (TypeError, ValueError) as error:
        raise ValueError("Port must be a number between 1 and 65535.") from error
    if not 1 <= port <= 65535:
        raise ValueError("Port must be between 1 and 65535.")
    return port


app = Flask(__name__)
controller = NodeController()


@app.get("/")
def dashboard():
    return render_template("index.html")


@app.get("/api/status")
def status():
    return jsonify(controller.snapshot())


@app.post("/api/node/start")
def start_node():
    try:
        payload = request.get_json(silent=True) or {}
        controller.start(request_value(payload, "host", "127.0.0.1"), request_port(payload))
        return jsonify(controller.snapshot())
    except (ValueError, TimeoutError, OSError) as error:
        return jsonify(error=str(error)), 400


@app.post("/api/node/stop")
def stop_node():
    controller.stop()
    return jsonify(controller.snapshot())


@app.post("/api/peers/ping")
def ping_peer():
    try:
        payload = request.get_json(silent=True) or {}
        controller.ping(request_value(payload, "host"), request_port(payload))
        return jsonify(controller.snapshot())
    except ValueError as error:
        return jsonify(error=str(error)), 400


@app.post("/api/tasks/submit")
def submit_task():
    try:
        payload = request.get_json(silent=True) or {}
        operation, first = request_value(payload, "operation"), request_value(payload, "first")
        second = payload.get("second", "")
        if operation != "echo":
            try:
                first, second = float(first), float(second)
            except (TypeError, ValueError) as error:
                raise ValueError("Add and multiply tasks require two numbers.") from error
        routing = str(payload.get("routing", "direct")).strip().lower()
        if routing not in {"direct", "least-loaded"}:
            raise ValueError("Routing mode must be direct or least-loaded.")
        host = payload.get("host", "127.0.0.1")
        port = payload.get("port", 4801)
        if routing == "direct":
            host, port = request_value(payload, "host"), request_port(payload)
        else:
            port = request_port({"port": port})
        controller.send_task(operation, first, second, host, port, routing)
        return jsonify(controller.snapshot())
    except ValueError as error:
        return jsonify(error=str(error)), 400


if __name__ == "__main__":
    app.run(port=5000)
