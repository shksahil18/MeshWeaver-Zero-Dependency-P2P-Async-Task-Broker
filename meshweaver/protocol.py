import json


def encode_message(message: dict) -> bytes:
    """
    Convert a Python dictionary into UTF-8 encoded JSON bytes.
    """
    return json.dumps(message).encode("utf-8")


def decode_message(data: bytes) -> dict:
    """
    Convert UTF-8 encoded JSON bytes back into a Python dictionary.
    """
    return json.loads(data.decode("utf-8"))