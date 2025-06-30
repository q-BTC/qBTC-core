"""
Node startup and shutdown procedures
"""

import os
import asyncio
import json
from log_utils import get_logger
from events.event_bus import event_bus

logger = get_logger(__name__)

async def startup(args=None):
    """Initialize node components"""
    logger.info("Starting node initialization")
    
    try:
        # Initialize database
        from database.database import set_db, get_db
        db_path = os.environ.get('ROCKSDB_PATH', './ledger.rocksdb')
        set_db(db_path)
        db = get_db()
        logger.info(f"Database initialized at {db_path}")
        
        # Start event bus if not already started
        if not event_bus.running:
            await event_bus.start()
            logger.info("Event bus started")
        
        # Initialize blockchain components
        from blockchain.chain_manager import ChainManager
        from blockchain.blockchain import Block, sha256d, calculate_merkle_root
        from config.config import GENESIS_ADDRESS, ADMIN_ADDRESS
        import time
        
        # Check if genesis block exists
        cm = ChainManager()
        best_hash, best_height = cm.get_best_chain_tip()
        
        if best_hash == "00" * 32 and best_height == 0:
            # No blocks exist, create genesis block
            logger.info("Creating genesis block...")
            
            # Create genesis transaction (21M coins to admin)
            genesis_tx = {
                "version": 1,
                "inputs": [{
                    "txid": "00" * 32,
                    "utxo_index": 0,
                    "signature": "",
                    "pubkey": ""
                }],
                "outputs": [{
                    "sender": GENESIS_ADDRESS,
                    "receiver": ADMIN_ADDRESS,
                    "amount": "21000000"  # 21 million coins
                }],
                "txid": sha256d(f"genesis_tx_{ADMIN_ADDRESS}".encode()).hex()
            }
            
            # Create genesis block
            genesis_block = Block(
                version=1,
                prev_block_hash="00" * 32,
                merkle_root=calculate_merkle_root([genesis_tx["txid"]]),
                timestamp=int(time.time()),
                bits=0x1d00ffff,  # Initial difficulty
                nonce=0
            )
            
            # Genesis block doesn't need PoW
            genesis_block_data = {
                "version": genesis_block.version,
                "previous_hash": genesis_block.prev_block_hash,
                "merkle_root": genesis_block.merkle_root,
                "timestamp": genesis_block.timestamp,
                "bits": genesis_block.bits,
                "nonce": genesis_block.nonce,
                "block_hash": "0" * 64,  # Special genesis hash
                "height": 0,
                "tx_ids": [genesis_tx["txid"]],
                "full_transactions": [genesis_tx]
            }
            
            # Add genesis block to chain
            success, error = cm.add_block(genesis_block_data)
            if success:
                logger.info("Genesis block created successfully")
                # Don't manually create the genesis UTXO - ChainManager already does this
                # when processing the genesis block transactions
                logger.info(f"Genesis block added with 21M coins to {ADMIN_ADDRESS}")
            else:
                logger.error(f"Failed to create genesis block: {error}")
        
        logger.info("Blockchain components ready")
        
        # Load wallet if specified
        wallet_file = os.environ.get('WALLET_FILE', 'wallet.json')
        wallet_password = os.environ.get('WALLET_PASSWORD', 'password123')
        
        from wallet.wallet import get_or_create_wallet
        wallet = get_or_create_wallet(fname=wallet_file, password=wallet_password)
        logger.info(f"Wallet loaded: {wallet['address']}")
        
        # Store in app state for access
        import sys
        sys.modules['__main__'].wallet = wallet
        
        # Initialize networking components (mandatory)
        if args:
            logger.info("Initializing networking components")
            logger.info(f"Received args object: {args}")
            # Import required modules
            from dht.dht import run_kad_server
            from gossip.gossip import GossipNode
            from config.config import VALIDATOR_ID
            
            # Create gossip node first
            logger.info(f"Creating Gossip node with ID {VALIDATOR_ID} on port {args.gossip_port}")
            logger.info(f"Bootstrap mode: {args.bootstrap}")
            gossip_node = GossipNode(
                node_id=VALIDATOR_ID,
                wallet=wallet,
                is_bootstrap=args.bootstrap,
                is_full_node=True
            )
            
            # Start DHT with gossip node reference
            logger.info(f"Starting DHT on port {args.dht_port}")
            bootstrap_addr = None
            if not args.bootstrap:
                # Connect to bootstrap server
                bootstrap_addr = [(args.bootstrap_server, args.bootstrap_port)]
                logger.info(f"Will connect to bootstrap server at {args.bootstrap_server}:{args.bootstrap_port}")
            
            # Determine external IP
            external_ip = args.external_ip
            if not external_ip:
                # In Docker, try to get container name as IP
                import socket
                try:
                    external_ip = socket.gethostname()
                    logger.info(f"Using hostname as external IP: {external_ip}")
                except:
                    external_ip = '0.0.0.0'
            
            dht_task = asyncio.create_task(
                run_kad_server(
                    port=args.dht_port,
                    bootstrap_addr=bootstrap_addr,
                    wallet=wallet,
                    gossip_node=gossip_node,
                    ip_address=external_ip,
                    gossip_port=args.gossip_port
                )
            )
            sys.modules['__main__'].dht_task = dht_task
            logger.info("DHT server started")
            
            # Start gossip server
            logger.info(f"Starting Gossip server on port {args.gossip_port}")
            gossip_task = asyncio.create_task(gossip_node.start_server(
                host='0.0.0.0',
                port=args.gossip_port
            ))
            sys.modules['__main__'].gossip_node = gossip_node
            sys.modules['__main__'].gossip_task = gossip_task
            
            # Also set in web module for health checks
            try:
                from web.web import set_gossip_node
                set_gossip_node(gossip_node)
                logger.info("Gossip node reference set in web module")
            except Exception as e:
                logger.warning(f"Could not set gossip node in web module: {e}")
            
            logger.info("Gossip server started")
            
            # Allow time for networking to initialize
            await asyncio.sleep(3)
            logger.info("Networking components initialized")
        else:
            logger.error("Network configuration required but no args provided")
            raise RuntimeError("Cannot start node without networking configuration")
        
        logger.info("Node startup completed")
        
    except Exception as e:
        logger.error(f"Failed to start node: {str(e)}")
        raise

async def shutdown():
    """Cleanup node components"""
    logger.info("Starting node shutdown")
    
    try:
        import sys
        
        # Stop gossip node if running
        if hasattr(sys.modules['__main__'], 'gossip_node'):
            gossip_node = sys.modules['__main__'].gossip_node
            await gossip_node.stop()
            logger.info("Gossip node stopped")
            
        # Cancel gossip task if running
        if hasattr(sys.modules['__main__'], 'gossip_task'):
            gossip_task = sys.modules['__main__'].gossip_task
            gossip_task.cancel()
            try:
                await gossip_task
            except asyncio.CancelledError:
                pass
            logger.info("Gossip task cancelled")
        
        # Cancel DHT task if running
        if hasattr(sys.modules['__main__'], 'dht_task'):
            dht_task = sys.modules['__main__'].dht_task
            dht_task.cancel()
            try:
                await dht_task
            except asyncio.CancelledError:
                pass
            logger.info("DHT task cancelled")
        
        # Stop event bus
        if event_bus.running:
            await event_bus.stop()
            logger.info("Event bus stopped")
        
        # Close database connections
        # Database cleanup happens automatically
        
        logger.info("Node shutdown completed")
        
    except Exception as e:
        logger.error(f"Error during shutdown: {str(e)}")
        # Don't raise during shutdown to allow graceful exit
