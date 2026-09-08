import asyncio
import socket

from meshweaver.node import MeshNode


def logistic_regression_predict(
    weights,
    bias,
    samples,
):
    """
    Pure-Python logistic regression inference.

    This demonstrates transmission and remote execution
    of a non-trivial ML-style mathematical function.
    """

    predictions = []

    for sample in samples:

        # Weighted sum
        score = sum(
            weight * feature
            for weight, feature in zip(
                weights,
                sample,
            )
        ) + bias

        # Sigmoid activation
        probability = (
            1 / (1 + pow(2.718281828459045, -score))
        )

        predictions.append(
            {
                "score": round(score, 6),
                "probability": round(
                    probability,
                    6,
                ),
                "prediction": int(
                    probability >= 0.5
                ),
            }
        )

    return predictions


def get_free_port(host):
    sock = socket.socket(
        socket.AF_INET,
        socket.SOCK_DGRAM,
    )

    sock.bind(
        (host, 0)
    )

    port = sock.getsockname()[1]

    sock.close()

    return port


async def main():

    host = socket.gethostbyname(
        "localhost"
    )

    sender = MeshNode(
        host=host,
        port=get_free_port(host),
    )

    receiver = MeshNode(
        host=host,
        port=get_free_port(host),
    )

    await sender.start()
    await receiver.start()

    print()
    print("=" * 70)
    print("       MeshWeaver ML Serialization Demonstration")
    print("=" * 70)

    print()
    print(
        f"[SENDER]   {sender.host}:{sender.port}"
    )

    print(
        f"[RECEIVER] {receiver.host}:{receiver.port}"
    )

    # ------------------------------------------------------------
    # ML model parameters
    # ------------------------------------------------------------

    weights = (
        1.25,
        -0.75,
        0.50,
    )

    bias = -0.20

    samples = (
        (2.0, 1.0, 3.0),
        (0.5, 2.0, 1.0),
        (3.0, 0.5, 2.5),
        (1.0, 3.0, 0.5),
    )

    print()
    print("[TASK] Logistic regression inference")
    print(
        f"[TASK] Samples: {len(samples)}"
    )

    # ------------------------------------------------------------
    # Send complex function to remote node
    # ------------------------------------------------------------

    task_id = sender.send_task(
        logistic_regression_predict,
        args=(
            weights,
            bias,
            samples,
        ),
        kwargs={},
        host=receiver.host,
        port=receiver.port,
    )

    print()
    print(
        f"[TASK] Dispatched task: {task_id}"
    )

    print(
        "[TASK] Waiting for remote execution..."
    )

    await asyncio.sleep(3)

    print()
    print("=" * 70)
    print("ML SERIALIZATION DEMONSTRATION COMPLETE")
    print("=" * 70)

    await sender.stop()
    await receiver.stop()


if __name__ == "__main__":
    try:
        asyncio.run(main())

    except KeyboardInterrupt:
        print("\nDemo interrupted.")