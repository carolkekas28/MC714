import asyncio
import logging
import os

from bully import BullyElection
from config import load_config
from lamport import LamportClock
from messages import Message, MessageType
from ricart_agrawala import RicartAgrawala
from transport import MessageTransport


class NodeIdFilter(logging.Filter):
    def __init__(self, node_id: int) -> None:
        super().__init__()
        self.node_id = node_id

    def filter(self, record: logging.LogRecord) -> bool:
        record.node_id = self.node_id
        return True


def configure_logging(node_id: int) -> None:
    import sys

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(
        logging.Formatter(
            "%(asctime)s [node%(node_id)s] %(levelname)s %(message)s"
        )
    )
    handler.addFilter(NodeIdFilter(node_id))
    logging.basicConfig(level=logging.INFO, handlers=[handler], force=True)


async def mutex_demo_loop(
    mutex: RicartAgrawala,
    transport: MessageTransport,
    logger: logging.Logger,
) -> None:
    config = transport.config
    await transport.wait_until_ready()
    await asyncio.sleep(5 + config.node_id)

    for round_index in range(3):
        logger.info("Mutex demo round %d: requesting critical section", round_index + 1)
        await mutex.request_critical_section(hold_time=2.0)
        await asyncio.sleep(4 + config.node_id)


async def run_node() -> None:
    config = load_config()
    configure_logging(config.node_id)
    logger = logging.getLogger(__name__)

    critical_log_path = os.environ.get(
        "CRITICAL_LOG_PATH", "shared/critical.log"
    )
    run_mutex_demo = os.environ.get("RUN_MUTEX_DEMO", "true").lower() == "true"

    logger.info(
        "Starting node %d/%d on %s:%d",
        config.node_id,
        config.node_count,
        config.listen_host,
        config.listen_port,
    )

    clock = LamportClock()
    mutex: RicartAgrawala | None = None
    bully: BullyElection | None = None

    async def handle_message(message: Message) -> None:
        if message.type == MessageType.HELLO:
            return

        if bully is not None:
            if message.type == MessageType.ELECTION:
                await bully.handle_election(message)
            elif message.type == MessageType.OK:
                await bully.handle_ok(message)
            elif message.type == MessageType.COORDINATOR:
                await bully.handle_coordinator(message)
            elif message.type == MessageType.HEARTBEAT:
                await bully.handle_heartbeat(message)

        if mutex is None:
            return
        if message.type == MessageType.REQUEST:
            await mutex.handle_request(message)
        elif message.type == MessageType.REPLY:
            await mutex.handle_reply(message)

    transport = MessageTransport(config, clock, handle_message)
    mutex = RicartAgrawala(config, transport, clock, critical_log_path)
    bully = BullyElection(config, transport)
    await transport.start()
    await bully.start()

    tasks: list[asyncio.Task[None]] = []
    if run_mutex_demo:
        tasks.append(
            asyncio.create_task(
                mutex_demo_loop(mutex, transport, logger),
                name="mutex-demo",
            )
        )

    try:
        if tasks:
            await asyncio.gather(*tasks)
        else:
            while True:
                await asyncio.sleep(3600)
    except asyncio.CancelledError:
        for task in tasks:
            task.cancel()
        await bully.stop()
        await transport.stop()


def main() -> None:
    try:
        asyncio.run(run_node())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
