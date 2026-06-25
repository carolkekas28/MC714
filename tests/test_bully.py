import pytest

from bully import BullyElection
from lamport import LamportClock
from messages import Message, MessageType

from helpers import MockTransport, make_config


@pytest.fixture
def node1_bully():
    config = make_config(node_id=1, node_count=4, base_port=9000)
    transport = MockTransport(config=config, clock=LamportClock())
    bully = BullyElection(config, transport)
    return bully, transport


def test_higher_peers(node1_bully):
    bully, _transport = node1_bully
    assert bully.higher_peers() == [2, 3]


@pytest.mark.asyncio
async def test_handle_coordinator_updates_leader(node1_bully):
    bully, _transport = node1_bully
    await bully.handle_coordinator(
        Message(type=MessageType.COORDINATOR, sender=3, lamport_ts=1)
    )

    assert bully.leader_id == 3
    assert bully._leader_known is True
    assert bully.election_in_progress is False


@pytest.mark.asyncio
async def test_handle_election_from_lower_does_not_restart_if_already_leader(
    node1_bully,
):
    bully, transport = node1_bully
    bully.leader_id = 1
    bully._leader_known = True

    await bully.handle_election(
        Message(type=MessageType.ELECTION, sender=0, lamport_ts=2)
    )

    assert len(transport.sent_messages) == 1
    peer, reply = transport.sent_messages[0]
    assert peer == 0
    assert reply.type == MessageType.OK
    assert reply.sender == 1
    assert bully.election_in_progress is False
