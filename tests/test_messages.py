import json

import pytest

from messages import (
    MAX_MESSAGE_SIZE,
    Message,
    MessageType,
    decode_message,
    encode_message,
)


def test_encode_decode_roundtrip():
    message = Message(
        type=MessageType.REQUEST,
        sender=2,
        lamport_ts=7,
        payload={"key": "value"},
    )
    decoded = decode_message(encode_message(message))
    assert decoded.type == MessageType.REQUEST
    assert decoded.sender == 2
    assert decoded.lamport_ts == 7
    assert decoded.payload == {"key": "value"}


def test_message_types_are_json_strings():
    payload = json.loads(encode_message(
        Message(type=MessageType.HEARTBEAT, sender=0)
    ).decode())
    assert payload["type"] == "HEARTBEAT"


def test_decode_rejects_oversized_payload():
    with pytest.raises(ValueError, match="exceeds maximum size"):
        decode_message(b"x" * (MAX_MESSAGE_SIZE + 1))
