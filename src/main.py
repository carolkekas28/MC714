import asyncio
import logging
import sys

from config import load_config
from lamport import LamportClock
from messages import Message, MessageType
from transport import MessageTransport


class NodeIdFilter(logging.Filter):
    def __init__(self, node_id: int) -> None:
        super().__init__()
        self.node_id = node_id

    def filter(self, record: logging.LogRecord) -> bool:
        record.node_id = self.node_id
        return True


def configure_logging(node_id: int) -> None:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(
        logging.Formatter(
            "%(asctime)s [node%(node_id)s] %(levelname)s %(message)s"
        )
    )
    handler.addFilter(NodeIdFilter(node_id))
    logging.basicConfig(level=logging.INFO, handlers=[handler], force=True)


async def lamport_demo_loop(
    transport: MessageTransport,
    clock: LamportClock,
    logger: logging.Logger,
) -> None:
    config = transport.config
    await transport.wait_until_ready()
    await asyncio.sleep(2 + config.node_id)

    while True:
        local_ts = clock.local_event()
        logger.info("Local event (Lamport ts=%d)", local_ts)

        target = (config.node_id + 1) % config.node_count
        if target != config.node_id:
            await transport.send(
                target,
                Message(
                    type=MessageType.EVENT,
                    sender=config.node_id,
                    payload={"description": f"event from node{config.node_id}"},
                ),
            )

        await asyncio.sleep(4 + config.node_id)


async def run_node() -> None:
    config = load_config()
    configure_logging(config.node_id)
    logger = logging.getLogger(__name__)

    logger.info(
        "Starting node %d/%d on %s:%d",
        config.node_id,
        config.node_count,
        config.listen_host,
        config.listen_port,
    )

    clock = LamportClock()

    async def handle_message(message: Message) -> None:
        if message.type == MessageType.HELLO:
            return
        if message.type == MessageType.EVENT:
            logger.info(
                "Observed remote event from node%d at Lamport ts=%d: %s",
                message.sender,
                message.lamport_ts,
                message.payload.get("description", ""),
            )

    transport = MessageTransport(config, clock, handle_message)
    await transport.start()

    demo_task = asyncio.create_task(
        lamport_demo_loop(transport, clock, logger),
        name="lamport-demo",
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
