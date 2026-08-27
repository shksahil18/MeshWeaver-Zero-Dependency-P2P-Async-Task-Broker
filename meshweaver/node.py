import asyncio
import uuid

from meshweaver.network import UDPNetwork
from meshweaver.protocol import (
    encode_message,
    decode_message,
)
from meshweaver.tasks.serializer import (
    serialize_task,
    execute_task,
)


class MeshNode:
    """
    Represents one MeshWeaver peer node.
    """

    def __init__(self, host: str, port: int):

        self.node_id = str(uuid.uuid4())[:8]

        self.host = host
        self.port = port

        self.network = UDPNetwork(
            host=self.host,
            port=self.port,
            on_message=self.handle_message,
        )

    async def start(self):
        """
        Start the node's UDP networking layer.
        """

        await self.network.start()

        print()
        print("=" * 55)
        print("          MeshWeaver Node Started")
        print("=" * 55)
        print(f"Node ID : {self.node_id}")
        print(f"Address : {self.host}:{self.port}")
        print("=" * 55)
        print()

    def handle_message(self, data: bytes, addr):
        """
        Process an incoming UDP message.
        """

        try:
            message = decode_message(data)

        except Exception as exc:
            print(
                f"[ERROR] Invalid message from {addr}: {exc}"
            )
            return

        message_type = message.get("type")

        if message_type == "PING":
            self.handle_ping(message, addr)

        elif message_type == "PONG":
            self.handle_pong(message, addr)

        elif message_type == "TASK":
            self.handle_task(message, addr)

        elif message_type == "TASK_RESULT":
            self.handle_task_result(message, addr)

        else:
            print(
                f"[WARNING] Unknown message type: "
                f"{message_type}"
            )

    def handle_ping(self, message, addr):
        """
        Respond to a PING message.
        """

        sender_id = message.get("node_id")

        print(
            f"[PING] Received from Node "
            f"{sender_id} @ {addr}"
        )

        response = {
            "type": "PONG",
            "node_id": self.node_id,
        }

        self.network.send(
            encode_message(response),
            addr[0],
            addr[1],
        )

        print(
            f"[PONG] Sent to Node {sender_id}"
        )

    def handle_pong(self, message, addr):
        """
        Handle PONG response.
        """

        sender_id = message.get("node_id")

        print(
            f"[PONG] Received from Node "
            f"{sender_id} @ {addr}"
        )

    def handle_task(self, message, addr):
        """
        Deserialize and execute a remote task.
        """

        sender_id = message.get("sender")

        print()
        print(
            f"[TASK] Received from Node "
            f"{sender_id} @ {addr}"
        )

        try:
            payload = bytes.fromhex(
                message["payload"]
            )

            result = execute_task(payload)

            response = {
                "type": "TASK_RESULT",
                "success": True,
                "result": result,
            }

            print(
                f"[TASK] Execution successful"
            )
            print(
                f"[TASK] Result: {result}"
            )

        except Exception as exc:

            response = {
                "type": "TASK_RESULT",
                "success": False,
                "error": str(exc),
            }

            print(
                f"[TASK] Execution failed: {exc}"
            )

        self.network.send(
            encode_message(response),
            addr[0],
            addr[1],
        )

    def handle_task_result(self, message, addr):
        """
        Handle the result of a remotely executed task.
        """

        if message.get("success"):

            print()
            print(
                "[TASK RESULT] Remote task completed"
            )

            print(
                f"[TASK RESULT] "
                f"{message.get('result')}"
            )

        else:

            print()
            print(
                "[TASK RESULT] Remote task failed"
            )

            print(
                f"[TASK ERROR] "
                f"{message.get('error')}"
            )

    def ping(self, host: str, port: int):
        """
        Send a PING message to another peer.
        """

        message = {
            "type": "PING",
            "node_id": self.node_id,
        }

        self.network.send(
            encode_message(message),
            host,
            port,
        )

        print(
            f"[PING] Sent to {host}:{port}"
        )

    def send_task(
        self,
        function,
        args=(),
        kwargs=None,
        host=None,
        port=None,
    ):
        """
        Serialize and send a Python function
        to another MeshWeaver node.
        """

        payload = serialize_task(
            function,
            args,
            kwargs,
        )

        message = {
            "type": "TASK",
            "sender": self.node_id,
            "payload": payload.hex(),
        }

        self.network.send(
            encode_message(message),
            host,
            port,
        )

        print(
            f"[TASK] Sent to {host}:{port}"
        )

    async def stop(self):
        """
        Stop the node.
        """

        self.network.close()

        print(
            f"[NODE] {self.node_id} stopped."
        )