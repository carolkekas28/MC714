import asyncio
import logging
import sys

from config import load_config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [node%(node_id)s] %(levelname)s %(message)s",
    stream=sys.stdout,
)


class NodeIdFilter(logging.Filter):
    def __init__(self, node_id: int) -> None:
        super().__init__()
        self.node_id = node_id

    def filter(self, record: logging.LogRecord) -> bool:
        record.node_id = self.node_id
        return True


async def run_node() -> None:
    config = load_config()
    logger = logging.getLogger(__name__)
    logger.addFilter(NodeIdFilter(config.node_id))

    logger.info(
        "Starting node %d/%d on %s:%d",
        config.node_id,
        config.node_count,
        config.listen_host,
        config.listen_port,
    )
    logger.info("Peers: %s", config.peers)
    logger.info("Waiting for transport layer (next implementation step)...")

    while True:
        await asyncio.sleep(3600)


def main() -> None:
    asyncio.run(run_node())


if __name__ == "__main__":
    main()
