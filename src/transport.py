import asyncio
import logging
from collections.abc import Awaitable, Callable

from config import NodeConfig
from lamport import LamportClock
from messages import Message, MessageType, decode_message, encode_message

MessageHandler = Callable[[Message], Awaitable[None]]

logger = logging.getLogger(__name__)


class MessageTransport:
    def __init__(
        self,
        config: NodeConfig,
        clock: LamportClock,
        on_message: MessageHandler,
    ) -> None:
        self.config = config
        self.clock = clock
        self.on_message = on_message
        self._writers: dict[int, asyncio.StreamWriter] = {}
        self._writer_lock = asyncio.Lock()
        self._server: asyncio.Server | None = None
        self._connect_tasks: list[asyncio.Task[None]] = []

    def peer_ids(self) -> list[int]:
        return [
            peer_id
            for peer_id in range(self.config.node_count)
            if peer_id != self.config.node_id
        ]

    async def start(self) -> None:
        self._server = await asyncio.start_server(
            self._handle_inbound_connection,
            self.config.listen_host,
            self.config.listen_port,
        )
        logger.info(
            "Listening on %s:%d",
            self.config.listen_host,
            self.config.listen_port,
        )

        for peer_id in self.peer_ids():
            if peer_id > self.config.node_id:
                task = asyncio.create_task(
                    self._connect_to_peer(peer_id),
                    name=f"connect-node{peer_id}",
                )
                self._connect_tasks.append(task)

    async def stop(self) -> None:
        for task in self._connect_tasks:
            task.cancel()
        await asyncio.gather(*self._connect_tasks, return_exceptions=True)

        async with self._writer_lock:
            for writer in self._writers.values():
                writer.close()
                try:
                    await writer.wait_closed()
                except OSError:
                    pass
            self._writers.clear()

        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()

    async def wait_until_ready(self, timeout: float = 120.0) -> None:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout

        while loop.time() < deadline:
            async with self._writer_lock:
                missing = [
                    peer_id
                    for peer_id in self.peer_ids()
                    if peer_id not in self._writers
                    or self._writers[peer_id].is_closing()
                ]
            if not missing:
                logger.info("All peer connections ready")
                return
            logger.info("Waiting for peers: %s", missing)
            await asyncio.sleep(1.0)

        raise TimeoutError(f"Timed out waiting for peers: {missing}")

    async def send(self, peer_id: int, message: Message) -> None:
        message.lamport_ts = self.clock.on_send()
        message.sender = self.config.node_id
        await self._write(peer_id, message)
        logger.info(
            "SEND -> node%d %s ts=%d",
            peer_id,
            message.type.value,
            message.lamport_ts,
        )

    async def broadcast(self, message: Message) -> None:
        message.lamport_ts = self.clock.on_send()
        message.sender = self.config.node_id
        delivered: list[int] = []
        for peer_id in self.peer_ids():
            try:
                await self._write(peer_id, message)
                delivered.append(peer_id)
            except ConnectionError:
                logger.warning("Broadcast skipped unreachable node%d", peer_id)
        logger.info(
            "BROADCAST %s ts=%d to %s",
            message.type.value,
            message.lamport_ts,
            delivered,
        )

    async def _write(self, peer_id: int, message: Message) -> None:
        payload = encode_message(message)
        async with self._writer_lock:
            writer = self._writers.get(peer_id)
            if writer is None or writer.is_closing():
                raise ConnectionError(f"No active connection to node{peer_id}")

            writer.write(len(payload).to_bytes(4, "big") + payload)
            await writer.drain()

    async def _connect_to_peer(self, peer_id: int) -> None:
        host, port = self.config.peer_address(peer_id)
        delay = 0.5

        while True:
            try:
                reader, writer = await asyncio.open_connection(host, port)
                await self._register_connection(peer_id, reader, writer)
                hello = Message(type=MessageType.HELLO, sender=self.config.node_id)
                hello.lamport_ts = self.clock.on_send()
                payload = encode_message(hello)
                writer.write(len(payload).to_bytes(4, "big") + payload)
                await writer.drain()
                logger.info("Connected to node%d at %s:%d", peer_id, host, port)
                await self._read_loop(peer_id, reader)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning(
                    "Connection to node%d failed (%s), retrying in %.1fs",
                    peer_id,
                    exc,
                    delay,
                )
                async with self._writer_lock:
                    self._writers.pop(peer_id, None)
                await asyncio.sleep(delay)
                delay = min(delay * 2, 5.0)

    async def _handle_inbound_connection(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        peer_address = writer.get_extra_info("peername")
        logger.info("Inbound connection from %s", peer_address)

        try:
            data = await self._read_frame(reader)
            message = decode_message(data)
            self.clock.on_receive(message.lamport_ts)

            if message.type != MessageType.HELLO:
                raise ValueError(
                    f"Expected HELLO as first message, got {message.type}"
                )

            peer_id = message.sender
            await self._register_connection(peer_id, reader, writer)
            logger.info("Handshake complete with node%d", peer_id)
            await self._read_loop(peer_id, reader)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning("Inbound connection from %s closed: %s", peer_address, exc)
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except OSError:
                pass

    async def _register_connection(
        self,
        peer_id: int,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        del reader
        async with self._writer_lock:
            existing = self._writers.get(peer_id)
            if existing is not None and not existing.is_closing():
                existing.close()
            self._writers[peer_id] = writer

    async def _read_loop(self, peer_id: int, reader: asyncio.StreamReader) -> None:
        while True:
            data = await self._read_frame(reader)
            message = decode_message(data)
            receive_ts = self.clock.on_receive(message.lamport_ts)
            logger.info(
                "RECV <- node%d %s ts=%d (local clock now %d)",
                peer_id,
                message.type.value,
                message.lamport_ts,
                receive_ts,
            )
            await self.on_message(message)

    async def _read_frame(self, reader: asyncio.StreamReader) -> bytes:
        length_bytes = await reader.readexactly(4)
        length = int.from_bytes(length_bytes, "big")
        if length <= 0 or length > 1_048_576:
            raise ValueError(f"Invalid frame length: {length}")
        return await reader.readexactly(length)
