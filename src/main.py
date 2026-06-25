import asyncio
import logging
import os

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

    logger.info(
        "Starting node %d/%d on %s:%d",
        config.node_id,
        config.node_count,
        config.listen_host,
        config.listen_port,
    )

    clock = LamportClock()
    mutex: RicartAgrawala | None = None

    async def handle_message(message: Message) -> None:
        if message.type == MessageType.HELLO:
            return
        if mutex is None:
            return
        if message.type == MessageType.REQUEST:
            await mutex.handle_request(message)
        elif message.type == MessageType.REPLY:
            await mutex.handle_reply(message)

    transport = MessageTransport(config, clock, handle_message)
    mutex = RicartAgrawala(config, transport, clock, critical_log_path)
    await transport.start()

    demo_task = asyncio.create_task(
        mutex_demo_loop(mutex, transport, logger),
        name="mutex-demo",
    )

    try:
        await demo_task
    except asyncio.CancelledError:
        demo_task.cancel()
        await transport.stop()


def main() -> None:
    try:
        asyncio.run(run_node())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
