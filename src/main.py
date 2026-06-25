import asyncio
import os

from config import load_config
from logging_utils import configure_logging
from node import Node, resolve_demo_mode


async def run_node() -> None:
    config = load_config()
    configure_logging(config.node_id)

    critical_log_path = os.environ.get(
        "CRITICAL_LOG_PATH", "shared/critical.log"
    )
    demo_mode = resolve_demo_mode()

    node = Node(config, critical_log_path)
    await node.start()

    try:
        await node.run_background_tasks(demo_mode)
    finally:
        await node.stop()


def main() -> None:
    try:
        asyncio.run(run_node())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
