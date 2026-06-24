import os
from dataclasses import dataclass


@dataclass(frozen=True)
class NodeConfig:
    node_id: int
    node_count: int
    base_port: int
    peers: dict[int, tuple[str, int]]

    @property
    def listen_host(self) -> str:
        return "0.0.0.0"

    @property
    def listen_port(self) -> int:
        return self.base_port + self.node_id

    def peer_address(self, peer_id: int) -> tuple[str, int]:
        return self.peers[peer_id]


def load_config() -> NodeConfig:
    node_id = int(os.environ["NODE_ID"])
    node_count = int(os.environ.get("NODE_COUNT", "4"))
    base_port = int(os.environ.get("BASE_PORT", "5000"))

    peers: dict[int, tuple[str, int]] = {}
    for index, entry in enumerate(os.environ["PEERS"].split(",")):
        host, port = entry.rsplit(":", 1)
        peers[index] = (host.strip(), int(port))

    if node_id not in peers:
        raise ValueError(f"NODE_ID {node_id} not found in PEERS")
    if len(peers) != node_count:
        raise ValueError(
            f"PEERS defines {len(peers)} nodes but NODE_COUNT is {node_count}"
        )

    return NodeConfig(
        node_id=node_id,
        node_count=node_count,
        base_port=base_port,
        peers=peers,
    )
