import json
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any


class MessageType(StrEnum):
    HELLO = "HELLO"
    EVENT = "EVENT"
    REQUEST = "REQUEST"
    REPLY = "REPLY"
    RELEASE = "RELEASE"
    ELECTION = "ELECTION"
    OK = "OK"
    COORDINATOR = "COORDINATOR"
    HEARTBEAT = "HEARTBEAT"


MAX_MESSAGE_SIZE = 1_048_576


@dataclass
class Message:
    type: MessageType
    sender: int
    lamport_ts: int = 0
    payload: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "type": self.type,
            "sender": self.sender,
            "lamport_ts": self.lamport_ts,
            "payload": self.payload,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Message":
        return cls(
            type=MessageType(data["type"]),
            sender=int(data["sender"]),
            lamport_ts=int(data.get("lamport_ts", 0)),
            payload=dict(data.get("payload", {})),
        )


def encode_message(message: Message) -> bytes:
    return json.dumps(message.to_dict(), separators=(",", ":")).encode("utf-8")


def decode_message(data: bytes) -> Message:
    if len(data) > MAX_MESSAGE_SIZE:
        raise ValueError(f"Message exceeds maximum size ({MAX_MESSAGE_SIZE} bytes)")
    return Message.from_dict(json.loads(data.decode("utf-8")))
