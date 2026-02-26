#!/usr/bin/env python3
"""
Web process entry point — runs FastAPI + WebSockets on a secondary (read-only) RocksDB.

Communicates with the node process via Redis:
  - qbtc:events     (pub/sub) — blockchain events forwarded to local event_bus
  - qbtc:db_updated (pub/sub) — triggers try_catch_up_with_primary()
  - qbtc:tx_submit  (list)    — transaction submission queue
  - qbtc:tx_result  (list)    — transaction result reply
  - qbtc:gossip_state (key)   — gossip network state snapshot

Does NOT import chain_manager, gossip, DHT, or mempool directly.
"""

import os
import asyncio
import logging
import uvicorn

from log_utils import setup_logging

logger = setup_logging(
    level="INFO",
    log_file="qbtc_web.log",
    enable_console=True,
    enable_structured=True,
)


async def periodic_db_catchup(interval: float = 1.0):
    """Safety-net: call try_catch_up_with_primary() every `interval` seconds."""
    from database.database import try_catch_up

    while True:
        try:
            try_catch_up()
        except Exception as e:
            logger.warning(f"Periodic DB catch-up failed: {e}")
        await asyncio.sleep(interval)


async def web_main():
    # ---- 1. Open RocksDB in secondary (read-only) mode ----
    from database.database import set_db_secondary

    db_path = os.environ.get("ROCKSDB_PATH", "/app/db")
    set_db_secondary(db_path)
    logger.info(f"Secondary database opened at {db_path}")

    # ---- 2. Set up Redis event bridge (web side) ----
    redis_url = os.environ.get("REDIS_URL")
    if not redis_url:
        logger.error("REDIS_URL environment variable is required for the web process")
        raise RuntimeError("REDIS_URL is required")

    from events.redis_event_bridge import WebEventBridge, web_bridge as _wb
    import events.redis_event_bridge as bridge_module

    bridge = WebEventBridge(redis_url)
    bridge_module.web_bridge = bridge  # make globally accessible

    # Store bridge reference in web.web module for endpoint access
    import web.web as web_module
    web_module._web_bridge = bridge
    web_module._is_web_process = True

    # ---- 3. Start event bus ----
    from events.event_bus import event_bus

    await event_bus.start()
    logger.info("Event bus started (web process)")

    # ---- 4. Start background tasks ----
    asyncio.create_task(bridge.subscribe_events())
    asyncio.create_task(bridge.subscribe_db_updates())
    asyncio.create_task(periodic_db_catchup(interval=1.0))
    logger.info("Redis subscriptions and periodic catch-up started")

    # ---- 5. Start Uvicorn (FastAPI) ----
    from web.web import app

    config = uvicorn.Config(
        app,
        host="0.0.0.0",
        port=8080,
        log_level="info",
        access_log=True,
    )
    server = uvicorn.Server(config)
    logger.info("Web server starting on port 8080")

    try:
        await server.serve()
    except asyncio.CancelledError:
        logger.info("Web server cancelled")
    finally:
        await bridge.close()
        await event_bus.stop()
        logger.info("Web process shutdown complete")


if __name__ == "__main__":
    try:
        asyncio.run(web_main())
    except KeyboardInterrupt:
        logger.info("Web process interrupted")
    except Exception as e:
        logger.critical(f"Web process fatal error: {e}")
        raise
