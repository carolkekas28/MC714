import asyncio
import logging
import os
from datetime import datetime, timezone

from config import NodeConfig
from lamport import LamportClock
from messages import Message, MessageType
from transport import MessageTransport

logger = logging.getLogger(__name__)


class RicartAgrawala:
    def __init__(
        self,
        config: NodeConfig,
        transport: MessageTransport,
        clock: LamportClock,
        critical_log_path: str,
    ) -> None:
        self.config = config
        self.transport = transport
        self.clock = clock
        self.critical_log_path = critical_log_path

        self.requesting = False
        self.in_critical_section = False
        self.request_ts: int | None = None
        self.replies_received: set[int] = set()
        self.deferred_replies: set[int] = set()
        self._lock = asyncio.Lock()
        self._enter_event = asyncio.Event()

    def _request_priority(self, ts: int, node_id: int) -> tuple[int, int]:
        return (ts, node_id)

    async def request_critical_section(self, hold_time: float = 2.0) -> None:
        async with self._lock:
            if self.requesting or self.in_critical_section:
                logger.warning("Critical section request ignored: already active")
                return

            self.requesting = True
            self.replies_received.clear()
            self._enter_event.clear()
            self.request_ts = self.clock.on_send()

        request = Message(
            type=MessageType.REQUEST,
            sender=self.config.node_id,
            lamport_ts=self.request_ts,
            payload={},
        )
        await self.transport.broadcast(request)

        logger.info(
            "REQUEST critical section (Lamport ts=%d), waiting for %d replies",
            self.request_ts,
            len(self.transport.peer_ids()),
        )

        try:
            await asyncio.wait_for(self._enter_event.wait(), timeout=30.0)
        except asyncio.TimeoutError:
            async with self._lock:
                self.requesting = False
                self.request_ts = None
            logger.error("Timed out waiting for critical section replies")
            return

        async with self._lock:
            self.requesting = False
            self.in_critical_section = True

        await self._use_critical_section(hold_time)
        await self.release_critical_section()

    async def handle_request(self, message: Message) -> None:
        requester = message.sender
        request_ts = message.lamport_ts

        reply_now = False
        async with self._lock:
            if (
                not self.in_critical_section
                and (
                    not self.requesting
                    or self._request_priority(request_ts, requester)
                    < self._request_priority(
                        self.request_ts or 0, self.config.node_id
                    )
                )
            ):
                reply_now = True
            else:
                self.deferred_replies.add(requester)
                logger.info(
                    "Deferred REPLY to node%d (request ts=%d)",
                    requester,
                    request_ts,
                )

        if reply_now:
            await self.transport.send(
                requester,
                Message(type=MessageType.REPLY, sender=self.config.node_id),
            )

    async def handle_reply(self, message: Message) -> None:
        async with self._lock:
            if not self.requesting:
                return

            self.replies_received.add(message.sender)
            logger.info(
                "Received REPLY from node%d (%d/%d)",
                message.sender,
                len(self.replies_received),
                len(self.transport.peer_ids()),
            )

            if len(self.replies_received) >= len(self.transport.peer_ids()):
                self._enter_event.set()

    async def release_critical_section(self) -> None:
        async with self._lock:
            if not self.in_critical_section:
                return

            self.in_critical_section = False
            deferred = sorted(self.deferred_replies)
            self.deferred_replies.clear()
            released_ts = self.request_ts
            self.request_ts = None

        logger.info("EXIT critical section (Lamport ts=%s)", released_ts)

        for peer_id in deferred:
            await self.transport.send(
                peer_id,
                Message(type=MessageType.REPLY, sender=self.config.node_id),
            )
            logger.info("Sent deferred REPLY to node%d", peer_id)

    async def _use_critical_section(self, hold_time: float) -> None:
        os.makedirs(os.path.dirname(self.critical_log_path) or ".", exist_ok=True)
        timestamp = datetime.now(timezone.utc).isoformat()

        def write_entry() -> None:
            with open(self.critical_log_path, "a", encoding="utf-8") as log_file:
                log_file.write(
                    f"{timestamp} node{self.config.node_id} "
                    f"lamport={self.request_ts}\n"
                )

        await asyncio.to_thread(write_entry)
        logger.info("ENTER critical section (Lamport ts=%d)", self.request_ts)
        await asyncio.sleep(hold_time)
