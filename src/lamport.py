class LamportClock:
    def __init__(self) -> None:
        self._time = 0

    @property
    def time(self) -> int:
        return self._time

    def local_event(self) -> int:
        self._time += 1
        return self._time

    def on_send(self) -> int:
        self._time += 1
        return self._time

    def on_receive(self, received_ts: int) -> int:
        self._time = max(self._time, received_ts) + 1
        return self._time
