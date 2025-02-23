# services/nsd_worker/app/main.py

import asyncio
import logging
import sys

from .config import NsdWorkerConfig
from .nsd_worker import NsdWorker

logger = logging.getLogger(__name__)

def main():
    """
    Entry point for nsd_worker. Creates NsdWorker, runs it, and handles Ctrl+C.
    """
    config = NsdWorkerConfig()
    worker = NsdWorker(config)

    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(worker.start())
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt: stopping worker...")
        worker.stop()
        loop.run_until_complete(worker.shutdown())
    finally:
        loop.close()

if __name__ == "__main__":
    main()
