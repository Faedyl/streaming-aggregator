"""
UAS Sistem Terdistribusi - Pub-Sub Log Aggregator
Entry point aplikasi
"""

import asyncio
import logging
import os
from src.app import create_app
from src.utils import setup_logging

logger = logging.getLogger(__name__)


async def main():
    """Main entry point"""
    log_level = os.getenv("LOG_LEVEL", "INFO")
    setup_logging(log_level)

    logger.info("=" * 60)
    logger.info("Starting UAS Pub-Sub Log Aggregator...")
    logger.info("=" * 60)

    app = create_app()

    import uvicorn

    config = uvicorn.Config(
        app,
        host="0.0.0.0",
        port=8080,
        log_level=log_level.lower()
    )

    server = uvicorn.Server(config)
    await server.serve()


if __name__ == "__main__":
    asyncio.run(main())
