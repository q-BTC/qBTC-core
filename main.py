#!/usr/bin/env python3

import asyncio
import argparse
import uvicorn
from web.web import app
from rpc.rpc import rpc_app
from node.startup import startup, shutdown
from log_utils import setup_logging
from config.config import RPC_PORT

# Setup structured logging
logger = setup_logging(
    level="INFO",
    log_file="qbtc.log",
    enable_console=True,
    enable_structured=True
)

async def main(args):
    logger.info("Starting qBTC-core node")
    logger.info(f"Mode: {'bootstrap' if args.bootstrap else 'peer'}")
    logger.info(f"Args: bootstrap={args.bootstrap}, dht_port={args.dht_port}, gossip_port={args.gossip_port}")
    logger.info(f"Bootstrap server: {args.bootstrap_server}:{args.bootstrap_port}")

    try:
        await startup(args)
        logger.info("Node startup completed successfully")
    except Exception as e:
        logger.error(f"Failed to start node: {str(e)}")
        raise

    config_web = uvicorn.Config(
        app,
        host="0.0.0.0",
        port=8080,
        log_level="info",
        access_log=True
    )
    server_web = uvicorn.Server(config_web)
    logger.info("Web server configured on port 8080")

    config_rpc = uvicorn.Config(
        rpc_app,
        host="0.0.0.0",
        port=RPC_PORT,
        log_level="info",
        access_log=True
    )
    server_rpc = uvicorn.Server(config_rpc)
    logger.info(f"RPC server configured on port {RPC_PORT}")

    try:
        logger.info("Starting web and RPC servers")
        await asyncio.gather(server_web.serve(), server_rpc.serve())
    except asyncio.CancelledError:
        logger.info("Servers cancelled, shutting down gracefully")
    except Exception as e:
        logger.error(f"Server error: {str(e)}")
        raise
    finally:
        logger.info("Initiating shutdown")
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

    args = parser.parse_args()

    try:
        asyncio.run(main(args))
    except KeyboardInterrupt:
        logger.info("Received interrupt signal, shutting down")
    except Exception as e:
        logger.critical(f"Fatal error: {str(e)}")
        raise
