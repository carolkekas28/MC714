from lamport import LamportClock


def test_local_event_increments_clock():
    clock = LamportClock()
    assert clock.time == 0
    assert clock.local_event() == 1
    assert clock.time == 1


def test_on_send_increments_clock():
    clock = LamportClock()
    assert clock.on_send() == 1
    assert clock.on_send() == 2


def test_on_receive_applies_max_plus_one():
    clock = LamportClock()
    clock.local_event()
    assert clock.on_receive(5) == 6
    assert clock.time == 6


def test_on_receive_does_not_go_backwards():
    clock = LamportClock()
    clock.on_receive(10)
    assert clock.time == 11
    assert clock.on_receive(3) == 12
