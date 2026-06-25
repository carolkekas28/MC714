import asyncio
import logging
import os
import sys

from bully import BullyElection
from config import NodeConfig
from lamport import LamportClock
from messages import Message, MessageType
from ricart_agrawala import RicartAgrawala
from transport import MessageTransport

logger = logging.getLogger(__name__)


def resolve_demo_mode() -> str:
    if "DEMO_MODE" in os.environ:
        return os.environ["DEMO_MODE"].lower()
    if os.environ.get("RUN_MUTEX_DEMO", "true").lower() == "false":
        return "none"
    return "mutex"


class Node:
    def __init__(self, config: NodeConfig, critical_log_path: str) -> None:
        self.config = config
        self.critical_log_path = critical_log_path
        self.clock = LamportClock()
        self.transport = MessageTransport(config, self.clock, self.handle_message)
        self.mutex = RicartAgrawala(
            config, self.transport, self.clock, critical_log_path
        )
        self.bully = BullyElection(config, self.transport)
        self._tasks: list[asyncio.Task[None]] = []

    async def start(self) -> None:
        logger.info(
            "Starting node %d/%d on %s:%d",
            self.config.node_id,
            self.config.node_count,
            self.config.listen_host,
            self.config.listen_port,
        )
        await self.transport.start()
        await self.bully.start()

    async def stop(self) -> None:
        for task in self._tasks:
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        await self.bully.stop()
        await self.transport.stop()

    async def handle_message(self, message: Message) -> None:
        if message.type == MessageType.HELLO:
            return

        if message.type == MessageType.EVENT:
            logger.info(
                "Observed remote event from node%d at Lamport ts=%d: %s",
                message.sender,
                message.lamport_ts,
                message.payload.get("description", ""),
            )
            return

        if message.type in {
            MessageType.ELECTION,
            MessageType.OK,
            MessageType.COORDINATOR,
            MessageType.HEARTBEAT,
        }:
            await self._handle_bully(message)
            return

        if message.type in {MessageType.REQUEST, MessageType.REPLY}:
            await self._handle_mutex(message)

    async def _handle_bully(self, message: Message) -> None:
        if message.type == MessageType.ELECTION:
            await self.bully.handle_election(message)
        elif message.type == MessageType.OK:
            await self.bully.handle_ok(message)
        elif message.type == MessageType.COORDINATOR:
            await self.bully.handle_coordinator(message)
        elif message.type == MessageType.HEARTBEAT:
            await self.bully.handle_heartbeat(message)

    async def _handle_mutex(self, message: Message) -> None:
        if message.type == MessageType.REQUEST:
            await self.mutex.handle_request(message)
        elif message.type == MessageType.REPLY:
            await self.mutex.handle_reply(message)

    async def request_critical_section(self, hold_time: float = 2.0) -> None:
        await self.mutex.request_critical_section(hold_time=hold_time)

    async def send_lamport_event(self, target_id: int | None = None) -> None:
        if target_id is None:
            target_id = (self.config.node_id + 1) % self.config.node_count

        local_ts = self.clock.local_event()
        logger.info("Local event (Lamport ts=%d)", local_ts)

        if target_id == self.config.node_id:
            return

        await self.transport.send(
            target_id,
            Message(
                type=MessageType.EVENT,
                sender=self.config.node_id,
                payload={
                    "description": f"event from node{self.config.node_id}",
                },
            ),
        )

    def status_summary(self) -> str:
        leader = (
            f"node{self.bully.leader_id}"
            if self.bully.leader_id is not None
            else "unknown"
        )
        return (
            f"node{self.config.node_id} | Lamport={self.clock.time} | "
            f"leader={leader} | in_cs={self.mutex.in_critical_section}"
        )

    async def run_background_tasks(self, demo_mode: str) -> None:
        if demo_mode == "mutex":
            self._tasks.append(
                asyncio.create_task(self._mutex_demo_loop(), name="mutex-demo")
            )
        elif demo_mode == "lamport":
            self._tasks.append(
                asyncio.create_task(self._lamport_demo_loop(), name="lamport-demo")
            )

        if sys.stdin.isatty():
            self._tasks.append(
                asyncio.create_task(self._cli_loop(), name="cli")
            )
        else:
            self._tasks.append(
                asyncio.create_task(self._command_file_loop(), name="command-file")
            )

        if not self._tasks:
            self._tasks.append(
                asyncio.create_task(self._idle_loop(), name="idle")
            )

        await asyncio.gather(*self._tasks)

    async def _idle_loop(self) -> None:
        while True:
            await asyncio.sleep(3600)

    async def _mutex_demo_loop(self) -> None:
        await self.transport.wait_until_ready()
        await asyncio.sleep(5 + self.config.node_id)

        for round_index in range(3):
            logger.info(
                "Mutex demo round %d: requesting critical section",
                round_index + 1,
            )
            await self.request_critical_section(hold_time=2.0)
            await asyncio.sleep(4 + self.config.node_id)

    async def _lamport_demo_loop(self) -> None:
        await self.transport.wait_until_ready()
        await asyncio.sleep(2 + self.config.node_id)

        while True:
            await self.send_lamport_event()
            await asyncio.sleep(4 + self.config.node_id)

    async def _command_file_loop(self) -> None:
        command_dir = os.environ.get("COMMAND_DIR", "shared/commands")
        os.makedirs(command_dir, exist_ok=True)
        command_path = os.path.join(
            command_dir, f"node{self.config.node_id}.cmd"
        )
        logger.info("Watching command file: %s", command_path)

        while True:
            if os.path.exists(command_path):
                command = await asyncio.to_thread(self._read_command_file, command_path)
                if command:
                    try:
                        await self._handle_cli_command(command)
                    except Exception as exc:
                        logger.error("Command failed: %s", exc)
            await asyncio.sleep(0.5)

    @staticmethod
    def _read_command_file(path: str) -> str:
        with open(path, encoding="utf-8") as command_file:
            command = command_file.read().strip()
        os.remove(path)
        return command

    async def _cli_loop(self) -> None:
        logger.info(
            "CLI ready: status | event [node] | request-cs | help | quit"
        )
        while True:
            line = await asyncio.to_thread(sys.stdin.readline)
            if not line:
                return

            command = line.strip().lower()
            if not command:
                continue

            try:
                await self._handle_cli_command(command)
            except Exception as exc:
                logger.error("Command failed: %s", exc)

            if command in {"quit", "exit"}:
                return

    async def _handle_cli_command(self, command: str) -> None:
        parts = command.split()
        name = parts[0]

        if name == "help":
            logger.info(
                "Commands: status, event [node], request-cs, quit"
            )
        elif name == "status":
            logger.info(self.status_summary())
        elif name == "event":
            target = int(parts[1]) if len(parts) > 1 else None
            await self.send_lamport_event(target)
        elif name in {"request-cs", "request_cs", "mutex"}:
            await self.request_critical_section()
        elif name in {"quit", "exit"}:
            pass
        else:
            logger.warning("Unknown command: %s (try help)", command)
