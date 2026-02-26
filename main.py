#!/usr/bin/env python3
"""
qBTC-core node entry point.

Two-process architecture:
  - Node process (this file): RocksDB primary (R/W), chain_manager, gossip, DHT, RPC
  - Web process (web_main.py): RocksDB secondary (RO), FastAPI, WebSockets

The web process is spawned as a separate OS process to eliminate GIL contention
between CPU-bound block validation and I/O-bound API request handling.
Cross-process communication happens via Redis pub/sub and queues.
"""

import asyncio
import argparse
import multiprocessing
import os
import uvicorn
from rpc.rpc import rpc_app
from node.startup import startup, shutdown
from log_utils import setup_logging

# Setup structured logging
logger = setup_logging(
    level="INFO",
    log_file="qbtc.log",
    enable_console=True,
    enable_structured=True
)


def _run_web():
    """Target for the web subprocess — runs web_main.web_main() in its own event loop."""
    import asyncio
    from web_main import web_main
    asyncio.run(web_main())


async def main(args):
    logger.info("Starting qBTC-core node (two-process mode)")
    logger.info(f"Mode: {'bootstrap' if args.bootstrap else 'peer'}")
    logger.info(f"Args: bootstrap={args.bootstrap}, dht_port={args.dht_port}, gossip_port={args.gossip_port}")
    logger.info(f"Bootstrap server: {args.bootstrap_server}:{args.bootstrap_port}")

    # ---- 1. Spawn the web process (separate GIL, separate event loop) ----
    web_proc = multiprocessing.Process(target=_run_web, daemon=True, name="qbtc-web")
    web_proc.start()
    logger.info(f"Web process spawned (pid={web_proc.pid})")

    # ---- 2. Configure RPC server (stays in node process) ----
    config_rpc = uvicorn.Config(
        rpc_app,
        host="0.0.0.0",
        port=8332,
        log_level="info",
        access_log=True
    )
    server_rpc = uvicorn.Server(config_rpc)
    logger.info("RPC server configured on port 8332")

    # ---- 3. Start RPC server and node initialization concurrently ----
    try:
        logger.info("Starting RPC server and node initialization concurrently")

        rpc_task = asyncio.create_task(server_rpc.serve())

        # Give server a moment to start listening
        await asyncio.sleep(0.5)
        logger.info("RPC server initialized, proceeding with node startup")

        # Run full node startup (DB, blockchain, gossip, DHT, etc.)
        startup_task = asyncio.create_task(startup(args))
        await startup_task
        logger.info("Node startup completed successfully")

        # ---- 4. Start node-side Redis event bridge ----
        redis_url = os.environ.get("REDIS_URL")
        if redis_url:
            import events.redis_event_bridge as bridge_module
            from events.redis_event_bridge import NodeEventBridge
            from node.node_event_publisher import (
                publish_gossip_state_periodic,
                publish_mempool_stats_periodic,
                listen_commit_submissions,
            )

            node_bridge = NodeEventBridge(redis_url)
            bridge_module.node_bridge = node_bridge  # make globally accessible

            asyncio.create_task(node_bridge.listen_tx_submissions())
            asyncio.create_task(publish_gossip_state_periodic(node_bridge, interval=10.0))
            asyncio.create_task(publish_mempool_stats_periodic(node_bridge, interval=5.0))
            asyncio.create_task(listen_commit_submissions(node_bridge))
            logger.info("Node-side Redis event bridge started")
        else:
            logger.warning("REDIS_URL not set — cross-process event bridge disabled")

        # ---- 5. Run until shutdown ----
        await rpc_task

    except asyncio.CancelledError:
        logger.info("Server cancelled, shutting down gracefully")
    except Exception as e:
        logger.error(f"Server/startup error: {str(e)}")
        raise
    finally:
        logger.info("Initiating shutdown")

        # Terminate web process
        if web_proc.is_alive():
            web_proc.terminate()
            web_proc.join(timeout=5)
            if web_proc.is_alive():
                web_proc.kill()
            logger.info("Web process terminated")

        await shutdown()
        logger.info("Shutdown completed")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='qBTC Node')
    parser.add_argument('--bootstrap', action='store_true',
                        help='Run as bootstrap server')
    parser.add_argument('--bootstrap_server', type=str, default='api.bitcoinqs.org',
                        help='Bootstrap server host (default: api.bitcoinqs.org)')
    parser.add_argument('--bootstrap_port', type=int, default=8001,
                        help='Bootstrap server port (default: 8001)')
    parser.add_argument('--dht-port', type=int, default=8001,
                        help='DHT port (default: 8001)')
    parser.add_argument('--gossip-port', type=int, default=8002,
                        help='Gossip port (default: 8002)')
    parser.add_argument('--external-ip', type=str, default=None,
                        help='External IP address for NAT traversal')
    parser.add_argument('--broadcast-transactions', action='store_true',
                        help='Enable websocket transaction broadcasting (optional for validators, mandatory for bootstrap)')

    args = parser.parse_args()

    # Check environment variable for broadcast_transactions
    if os.getenv('BROADCAST_TRANSACTIONS', '').lower() == 'true':
        args.broadcast_transactions = True

    # Make transaction broadcasting mandatory for bootstrap nodes
    if args.bootstrap:
        args.broadcast_transactions = True

    try:
        asyncio.run(main(args))
    except KeyboardInterrupt:
        logger.info("Received interrupt signal, shutting down")
    except Exception as e:
        logger.critical(f"Fatal error: {str(e)}")
        raise
