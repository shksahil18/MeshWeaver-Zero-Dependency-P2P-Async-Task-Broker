"""Flask UI adapter for MeshWeaver.

The adapter owns only web-dashboard state. It uses the public ``MeshNode``
interface and never changes the broker package itself.
"""

import asyncio
import threading
from datetime import datetime, timezone

from flask import Flask, jsonify, render_template, request

from meshweaver.node import MeshNode


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
        super().__init__(host, port)
        self._on_event = on_event

    def handle_pong(self, message, addr):
        super().handle_pong(message, addr)
        node_id_raw = message.get("node_id", "unknown")
        node_id_str = f"{node_id_raw:040x}" if isinstance(node_id_raw, int) else str(node_id_raw)
        self._on_event("peer", f"Heartbeat received from {node_id_str}", host=addr[0], port=addr[1], node_id=node_id_str)

    def handle_task_result(self, message, addr):
        super().handle_task_result(message, addr)
        is_success = bool(message.get("success"))
        result_val = message.get("result")
        detail = f"Remote task completed: {result_val!s}" if is_success else f"Remote task failed: {message.get('error', 'unknown error')}"
        self._on_event("success" if is_success else "error", detail, host=addr[0], port=addr[1], result=str(result_val) if is_success else None)


class NodeController:
    """Run one optional local node on a dedicated asyncio event-loop thread."""

    def __init__(self):
        self._lock = threading.RLock()
        self._node = None
        self._peers, self._events, self._tasks = {}, [], []
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
            if event_type == "peer" and extra.get("host"):
                key = f"{extra['host']}:{extra['port']}"
                self._peers[key] = {
                    "name": str(extra.get("node_id", "unknown peer")),
                    "host": str(extra["host"]),
                    "port": int(extra["port"]),
                    "status": "Online"
                }
            elif event_type in ("success", "error") and extra.get("host"):
                target = f"{extra['host']}:{extra['port']}"
                for task in self._tasks:
                    if task.get("target") == target and task.get("status") == "Dispatched":
                        task["status"] = "Completed" if event_type == "success" else "Failed"
                        if "result" in extra and extra["result"] is not None:
                            task["result"] = extra["result"]
                        break

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

    def ping(self, host, port):
        node = self._node_or_error()
        self._loop.call_soon_threadsafe(node.ping, host, port)
        self._event("send", f"PING sent to {host}:{port}")

    def send_task(self, operation, first, second, host, port):
        node = self._node_or_error()
        if operation not in TASKS:
            raise ValueError("Unknown task operation.")
        args = (first,) if operation == "echo" else (first, second)
        self._loop.call_soon_threadsafe(node.send_task, TASKS[operation], args, None, host, port)
        args_display = f"'{first}'" if operation == "echo" else f"{first}, {second}"
        task = {
            "name": operation,
            "args": args_display,
            "target": f"{host}:{port}",
            "time": self._timestamp(),
            "status": "Dispatched",
            "result": None
        }
        with self._lock:
            self._tasks.insert(0, task)
            del self._tasks[12:]
        self._event("send", f"{operation} task dispatched to {host}:{port}")

    def snapshot(self):
        with self._lock:
            node = self._node
            node_id_val = None
            if node is not None:
                node_id_val = getattr(node, "node_id_hex", None) or (f"{node.node_id:040x}" if isinstance(node.node_id, int) else str(node.node_id))
            return {
                "running": node is not None,
                "node_id": node_id_val,
                "address": f"{node.host}:{node.port}" if node else None,
                "peers": list(self._peers.values()),
                "events": list(self._events),
                "tasks": list(self._tasks),
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
        controller.send_task(operation, first, second, request_value(payload, "host"), request_port(payload))
        return jsonify(controller.snapshot())
    except ValueError as error:
        return jsonify(error=str(error)), 400


if __name__ == "__main__":
    app.run(port=5000)
