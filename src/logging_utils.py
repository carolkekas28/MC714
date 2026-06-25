import logging
import sys


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
