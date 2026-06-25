import asyncio
from pathlib import Path

import pytest

from messages import MessageType
from node import Node

from helpers import allocate_base_port, make_config


async def _start_cluster(
    tmp_path: Path,
    node_count: int,
    base_port: int,
    *,
    with_bully: bool = False,
) -> list[Node]:
    nodes: list[Node] = []
    critical_log = tmp_path / "critical.log"

    for node_id in range(node_count):
        config = make_config(node_id, node_count, base_port)
        node = Node(config, str(critical_log))
        await node.start(with_bully=with_bully)
        nodes.append(node)

    await nodes[0].transport.wait_until_ready()
    await asyncio.sleep(0.5)
    return nodes


async def _stop_cluster(nodes: list[Node]) -> None:
    for node in nodes:
        await node.stop()


@pytest.mark.asyncio
async def test_lamport_event_propagates_between_nodes(tmp_path):
    base_port = allocate_base_port()
    nodes = await _start_cluster(tmp_path, node_count=3, base_port=base_port)

    try:
        await nodes[0].send_lamport_event(target_id=1)
        await asyncio.sleep(0.5)

        assert nodes[0].clock.time >= 1
        assert nodes[1].clock.time >= 1
    finally:
        await _stop_cluster(nodes)


@pytest.mark.asyncio
async def test_mutex_serializes_three_node_access(tmp_path):
    base_port = allocate_base_port() + 10
    log_path = tmp_path / "critical.log"
    nodes = await _start_cluster(tmp_path, node_count=3, base_port=base_port)

    try:
        for node in nodes:
            await node.request_critical_section(hold_time=0.05)

        lines = log_path.read_text(encoding="utf-8").strip().splitlines()
        assert len(lines) == 3

        lamport_values = [
            int(line.split("lamport=")[1]) for line in lines
        ]
        assert len(set(lamport_values)) == 3
        assert lamport_values == sorted(lamport_values)
        assert all(not node.mutex.in_critical_section for node in nodes)
    finally:
        await _stop_cluster(nodes)


@pytest.mark.asyncio
async def test_bully_highest_node_becomes_initial_coordinator(tmp_path):
    base_port = allocate_base_port() + 20
    nodes = await _start_cluster(tmp_path, node_count=3, base_port=base_port)

    try:
        await nodes[2].bully._announce_coordinator()
        await asyncio.sleep(0.3)

        assert nodes[2].bully.leader_id == 2
        assert nodes[0].bully.leader_id == 2
        assert nodes[1].bully.leader_id == 2
    finally:
        await _stop_cluster(nodes)
