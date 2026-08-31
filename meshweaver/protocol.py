import json


def encode_message(message: dict) -> bytes:
    """
    Convert a Python dictionary into JSON bytes.
    """

    return json.dumps(
        message,
        separators=(",", ":"),
    ).encode("utf-8")


def decode_message(data: bytes) -> dict:
    """
    Convert JSON bytes back into a Python dictionary.
    """

    return json.loads(
        data.decode("utf-8")
    )