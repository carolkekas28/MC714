import pytest

from lamport import LamportClock
from messages import Message, MessageType
from ricart_agrawala import RicartAgrawala

from helpers import MockTransport, make_config


@pytest.fixture
def node1_mutex(tmp_path):
    clock = LamportClock()
    config = make_config(node_id=1, node_count=3, base_port=9000)
    transport = MockTransport(config=config, clock=clock)
    mutex = RicartAgrawala(
        config, transport, clock, str(tmp_path / "critical.log")
    )
    return mutex, transport


@pytest.mark.asyncio
async def test_handle_request_replies_when_idle(node1_mutex):
    mutex, transport = node1_mutex
    await mutex.handle_request(
        Message(type=MessageType.REQUEST, sender=0, lamport_ts=3)
    )

    assert len(transport.sent_messages) == 1
    peer, reply = transport.sent_messages[0]
    assert peer == 0
    assert reply.type == MessageType.REPLY
    assert reply.sender == 1
    assert mutex.deferred_replies == set()


@pytest.mark.asyncio
async def test_handle_request_defers_lower_priority(node1_mutex):
    mutex, transport = node1_mutex
    mutex.requesting = True
    mutex.request_ts = 5

    await mutex.handle_request(
        Message(type=MessageType.REQUEST, sender=2, lamport_ts=6)
    )

    assert transport.sent_messages == []
    assert mutex.deferred_replies == {2}


@pytest.mark.asyncio
async def test_handle_request_replies_to_higher_priority_request(node1_mutex):
    mutex, transport = node1_mutex
    mutex.requesting = True
    mutex.request_ts = 5

    await mutex.handle_request(
        Message(type=MessageType.REQUEST, sender=2, lamport_ts=4)
    )

    assert len(transport.sent_messages) == 1
    assert transport.sent_messages[0][0] == 2


@pytest.mark.asyncio
async def test_handle_reply_enters_critical_section(node1_mutex):
    mutex, _transport = node1_mutex
    mutex.requesting = True
    mutex.request_ts = 8

    await mutex.handle_reply(
        Message(type=MessageType.REPLY, sender=0, lamport_ts=9)
    )
    await mutex.handle_reply(
        Message(type=MessageType.REPLY, sender=2, lamport_ts=10)
    )

    assert mutex.replies_received == {0, 2}
    assert mutex._enter_event.is_set()
