#!/usr/bin/env python3

import asyncio
import argparse
import uvicorn
from web.web import app
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

async def main(args):
    logger.info("Starting qBTC-core node")
    logger.info(f"Mode: {'bootstrap' if args.bootstrap else 'peer'}")
    logger.info(f"Args: bootstrap={args.bootstrap}, dht_port={args.dht_port}, gossip_port={args.gossip_port}")
    logger.info(f"Bootstrap server: {args.bootstrap_server}:{args.bootstrap_port}")
    
    # Configure servers early (before heavy initialization)
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
        port=8332,
        log_level="info",
        access_log=True
    )
    server_rpc = uvicorn.Server(config_rpc)
    logger.info("RPC server configured on port 8332")
    
    # Start servers and node initialization concurrently
    try:
        logger.info("Starting web/RPC servers and node initialization concurrently")
        
        # Create background tasks for servers
        web_task = asyncio.create_task(server_web.serve())
        rpc_task = asyncio.create_task(server_rpc.serve())
        
        # Give servers a moment to start listening
        await asyncio.sleep(0.5)
        logger.info("Servers initialized, proceeding with node startup")
        
        # Create startup task
        startup_task = asyncio.create_task(startup(args))
        
        # Wait for startup to complete
        await startup_task
        logger.info("Node startup completed successfully")
        
        # Continue running servers
        await asyncio.gather(web_task, rpc_task)
        
    except asyncio.CancelledError:
        logger.info("Servers cancelled, shutting down gracefully")
    except Exception as e:
        logger.error(f"Server/startup error: {str(e)}")
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
    parser.add_argument('--broadcast-transactions', action='store_true',
                        help='Enable websocket transaction broadcasting (optional for validators, mandatory for bootstrap)')
    
    args = parser.parse_args()
    
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
