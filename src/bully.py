import asyncio
import logging
import os
import time

from config import NodeConfig
from messages import Message, MessageType
from transport import MessageTransport

logger = logging.getLogger(__name__)

HEARTBEAT_INTERVAL = float(os.environ.get("HEARTBEAT_INTERVAL", "1.0"))
LEADER_TIMEOUT = float(os.environ.get("LEADER_TIMEOUT", "3.0"))
ELECTION_TIMEOUT = float(os.environ.get("ELECTION_TIMEOUT", "2.0"))
STARTUP_GRACE = float(os.environ.get("STARTUP_GRACE", "3.0"))


class BullyElection:
    def __init__(self, config: NodeConfig, transport: MessageTransport) -> None:
        self.config = config
        self.transport = transport
        self.leader_id: int | None = None
        self._leader_known = False
        self.last_heartbeat = time.monotonic()
        self.election_in_progress = False
        self._got_ok_from_higher = False
        self._lock = asyncio.Lock()
        self._tasks: list[asyncio.Task[None]] = []

    def higher_peers(self) -> list[int]:
        return [
            peer_id
            for peer_id in range(self.config.node_id + 1, self.config.node_count)
        ]

    async def start(self) -> None:
        await self.transport.wait_until_ready()
        await asyncio.sleep(STARTUP_GRACE)

        if not self.higher_peers():
            await self._announce_coordinator()

        self._tasks.append(
            asyncio.create_task(self._heartbeat_loop(), name="bully-heartbeat")
        )
        self._tasks.append(
            asyncio.create_task(self._monitor_leader_loop(), name="bully-monitor")
        )

    async def stop(self) -> None:
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)

    async def start_election(self) -> None:
        async with self._lock:
            if self.election_in_progress:
                return
            self.election_in_progress = True
            self._got_ok_from_higher = False

        logger.info("Starting election")

        higher = self.higher_peers()
        if not higher:
            await self._announce_coordinator()
            return

        for peer_id in higher:
            try:
                await self.transport.send(
                    peer_id,
                    Message(type=MessageType.ELECTION, sender=self.config.node_id),
                )
            except ConnectionError:
                logger.info("No response path to node%d (may be down)", peer_id)

        await asyncio.sleep(ELECTION_TIMEOUT)

        async with self._lock:
            waiting_for_coordinator = (
                self.election_in_progress and self._got_ok_from_higher
            )

        if waiting_for_coordinator:
            logger.info("Waiting for coordinator announcement from higher node")
            deadline = time.monotonic() + ELECTION_TIMEOUT
            while time.monotonic() < deadline:
                async with self._lock:
                    if not self.election_in_progress:
                        return
                    if self.leader_id == self.config.node_id:
                        return
                await asyncio.sleep(0.2)

            async with self._lock:
                if self.election_in_progress:
                    logger.warning(
                        "Higher node did not announce coordinator; retrying takeover"
                    )
                    self._got_ok_from_higher = False

        async with self._lock:
            should_win = self.election_in_progress and not self._got_ok_from_higher

        if should_win:
            await self._announce_coordinator()
        else:
            async with self._lock:
                self.election_in_progress = False

    async def handle_election(self, message: Message) -> None:
        logger.info("Received ELECTION from node%d", message.sender)
        try:
            await self.transport.send(
                message.sender,
                Message(type=MessageType.OK, sender=self.config.node_id),
            )
        except ConnectionError:
            logger.warning("Failed to send OK to node%d", message.sender)

        async with self._lock:
            already_leader = (
                self._leader_known and self.leader_id == self.config.node_id
            )
        if not already_leader:
            await self.start_election()

    async def handle_ok(self, message: Message) -> None:
        if message.sender <= self.config.node_id:
            return

        async with self._lock:
            if self.election_in_progress:
                self._got_ok_from_higher = True
                logger.info(
                    "Received OK from higher node%d during election", message.sender
                )

    async def handle_coordinator(self, message: Message) -> None:
        async with self._lock:
            self.leader_id = message.sender
            self._leader_known = True
            self.election_in_progress = False
            self._got_ok_from_higher = False
            self.last_heartbeat = time.monotonic()

        logger.info("Accepted node%d as coordinator", message.sender)

    async def handle_heartbeat(self, message: Message) -> None:
        async with self._lock:
            self.leader_id = message.sender
            self._leader_known = True
            self.last_heartbeat = time.monotonic()

    async def _announce_coordinator(self) -> None:
        async with self._lock:
            self.leader_id = self.config.node_id
            self._leader_known = True
            self.election_in_progress = False
            self._got_ok_from_higher = False
            self.last_heartbeat = time.monotonic()

        await self.transport.broadcast(
            Message(type=MessageType.COORDINATOR, sender=self.config.node_id)
        )
        logger.info("I am the new coordinator (node%d)", self.config.node_id)

    async def _heartbeat_loop(self) -> None:
        while True:
            await asyncio.sleep(HEARTBEAT_INTERVAL)
            async with self._lock:
                is_leader = (
                    self._leader_known and self.leader_id == self.config.node_id
                )
            if not is_leader:
                continue

            await self.transport.broadcast(
                Message(type=MessageType.HEARTBEAT, sender=self.config.node_id)
            )

    async def _monitor_leader_loop(self) -> None:
        while True:
            await asyncio.sleep(0.5)
            async with self._lock:
                if not self._leader_known or self.leader_id is None:
                    continue
                is_follower = self.leader_id != self.config.node_id
                elapsed = time.monotonic() - self.last_heartbeat
                leader_id = self.leader_id

            if is_follower and elapsed > LEADER_TIMEOUT:
                logger.warning(
                    "Leader node%d timed out (%.1fs), starting election",
                    leader_id,
                    elapsed,
                )
                async with self._lock:
                    self.last_heartbeat = time.monotonic()
                await self.start_election()
