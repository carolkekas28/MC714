from __future__ import annotations

import os
from dataclasses import dataclass, field

from config import NodeConfig
from lamport import LamportClock
from messages import Message


def make_config(
    node_id: int,
    node_count: int,
    base_port: int,
    host: str = "127.0.0.1",
) -> NodeConfig:
    peers = {index: (host, base_port + index) for index in range(node_count)}
    return NodeConfig(
        node_id=node_id,
        node_count=node_count,
        base_port=base_port,
        peers=peers,
    )


def allocate_base_port() -> int:
    return 19000 + (os.getpid() % 500) * 10


@dataclass
class MockTransport:
    config: NodeConfig
    clock: LamportClock
    sent_messages: list[tuple[int, Message]] = field(default_factory=list)
    broadcast_messages: list[Message] = field(default_factory=list)

    def peer_ids(self) -> list[int]:
        return [
            peer_id
            for peer_id in range(self.config.node_count)
            if peer_id != self.config.node_id
        ]

    async def send(self, peer_id: int, message: Message) -> None:
        message.lamport_ts = self.clock.on_send()
        message.sender = self.config.node_id
        self.sent_messages.append((peer_id, message))

    async def broadcast(self, message: Message) -> None:
        message.lamport_ts = self.clock.on_send()
        message.sender = self.config.node_id
        self.broadcast_messages.append(message)

    async def wait_until_ready(self) -> None:
        return None

    async def start(self) -> None:
        return None

    async def stop(self) -> None:
        return None
