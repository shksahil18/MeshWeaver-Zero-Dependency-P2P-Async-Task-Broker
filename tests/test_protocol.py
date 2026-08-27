from meshweaver.protocol import (
    encode_message,
    decode_message,
)


def test_message_encoding_and_decoding():

    message = {
        "type": "PING",
        "node_id": "abc123",
    }

    encoded = encode_message(message)

    assert isinstance(encoded, bytes)

    decoded = decode_message(encoded)

    assert decoded == message