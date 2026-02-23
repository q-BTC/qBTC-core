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
        db_path = os.environ.get('ROCKSDB_PATH', '/app/db')
        set_db(db_path)
        db = get_db()
        logger.info(f"Database initialized at {db_path}")
        
        # Start event bus if not already started
        if not event_bus.running:
            await event_bus.start()
            logger.info("Event bus started")
        
        # Initialize blockchain components
        from blockchain.chain_singleton import get_chain_manager
        from blockchain.blockchain import Block, sha256d, calculate_merkle_root
        from blockchain.block_factory import create_block, create_genesis_block
        from config.config import GENESIS_ADDRESS, ADMIN_ADDRESS
        import time
        
        # Check current blockchain state
        cm = await get_chain_manager()
        best_hash, best_height = await cm.get_best_chain_tip()
        
        # Initialize height index
        from blockchain.block_height_index import get_height_index
        height_index = get_height_index()
        
        # Check if index needs to be rebuilt
        if best_height > 0:
            # Check if the index has the latest block
            latest_indexed = await height_index.get_highest_indexed_height()
            if latest_indexed < best_height:
                logger.info(f"Height index is behind (indexed: {latest_indexed}, chain: {best_height}). Scheduling background rebuild...")
                # Rebuild index in background to not block startup
                async def rebuild_index_async():
                    logger.info("Starting background height index rebuild...")
                    start_time = time.time()
                    await height_index.rebuild_index()
                    elapsed = time.time() - start_time
                    logger.info(f"Background height index rebuild complete in {elapsed:.2f} seconds")
                
                # Create background task for index rebuild
                asyncio.create_task(rebuild_index_async())
            else:
                logger.info(f"Height index is up to date (indexed: {latest_indexed}, chain: {best_height})")
            
            # Check wallet indexes and rebuild if needed
            logger.info("Checking wallet indexes...")
            from blockchain.wallet_index import get_wallet_index
            wallet_index = get_wallet_index()
            
            # Quick check: see if we have any wallet indexes at all
            stats = wallet_index.get_statistics()

            # DO NOT count all transactions - this is O(N)!
            # The wallet index will be built incrementally as needed
            if stats["wallet_tx_lists"] == 0:
                logger.info("No wallet indexes found. Will be built incrementally as transactions arrive.")
                # DO NOT rebuild! Indexes will be created incrementally
            else:
                logger.info(f"Wallet indexes present: {stats['wallet_tx_lists']} wallets indexed")
        
        # Check if database is truly empty (no blocks at all)
        has_any_blocks = False
        try:
            # Quick check for genesis block instead of iterating all keys
            genesis_key = b"block:" + ("0" * 64).encode()
            logger.info(f"[DEBUG] Checking for genesis key: {genesis_key}")
            # Check if genesis block actually exists and has data
            genesis_data = db.get(genesis_key)
            if genesis_data is not None and genesis_data != b'':
                logger.info(f"[DEBUG] Genesis key exists with data length: {len(genesis_data)}")
                has_any_blocks = True
            else:
                # Only check first few keys as a fallback
                key_count = 0
                for key, _ in db.items():
                    if key.startswith(b"block:"):
                        logger.info(f"[DEBUG] Found block key: {key}")
                        has_any_blocks = True
                        break
                    key_count += 1
                    if key_count > 100:  # Limit iteration
                        break
        except Exception as e:
            logger.warning(f"Could not check database for existing blocks: {e}")

        logger.info(f"[DEBUG] has_any_blocks={has_any_blocks} after checking database")

        if not has_any_blocks:
            # Database is completely empty - decide what to do based on node type
            if args and args.bootstrap:
                # Only bootstrap servers create genesis blocks
                logger.info("Bootstrap server starting with empty database - creating genesis block...")
                
                # Create genesis transaction (21M coins to admin)
                # First create transaction without txid
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
                    "body": {
                        "transaction_data": "initial_distribution",
                        "msg_str": "",  # No message for genesis
                        "pubkey": "",   # No pubkey for genesis
                        "signature": "" # No signature for genesis
                    }
                }
                
                # Calculate txid using same method as chain_manager
                from blockchain.blockchain import serialize_transaction
                tx_hex = serialize_transaction(genesis_tx)
                tx_bytes = bytes.fromhex(tx_hex)
                genesis_tx["txid"] = sha256d(tx_bytes)[::-1].hex()
                
                # Import MAX_TARGET_BITS to ensure genesis uses same difficulty as subsequent blocks
                from blockchain.difficulty import MAX_TARGET_BITS
                
                # Create genesis block
                genesis_block = Block(
                    version=1,
                    prev_block_hash="00" * 32,
                    merkle_root=calculate_merkle_root([genesis_tx["txid"]]),
                    timestamp=int(time.time()),
                    bits=MAX_TARGET_BITS,  # Use same difficulty as subsequent blocks
                    nonce=0
                )
                
                # Genesis block has special all-zeros hash
                genesis_block_hash = "0" * 64
                
                # Genesis block doesn't need PoW - use factory for consistency
                # Calculate initial difficulty for genesis
                from blockchain.blockchain import bits_to_target
                from decimal import Decimal
                target = bits_to_target(genesis_block.bits)
                initial_difficulty = Decimal(2**256) / Decimal(target)
                
                genesis_block_data = create_block(
                    version=genesis_block.version,
                    height=0,
                    block_hash=genesis_block_hash,
                    previous_hash=genesis_block.prev_block_hash,
                    bits=genesis_block.bits,
                    nonce=genesis_block.nonce,
                    timestamp=genesis_block.timestamp,
                    merkle_root=genesis_block.merkle_root,
                    tx_ids=[genesis_tx["txid"]],
                    full_transactions=[genesis_tx],
                    miner_address=None,  # No specific miner for genesis
                    connected=False,  # Will be connected when it becomes best tip
                    cumulative_difficulty=str(initial_difficulty)  # Genesis has its own difficulty
                )
                
                # Add genesis block to chain
                success, error = await cm.add_block(genesis_block_data)
                if success:
                    logger.info("Genesis block created successfully")
                    logger.info(f"Genesis block added with 21M coins to {ADMIN_ADDRESS}")
                else:
                    logger.error(f"Failed to create genesis block: {error}")
            else:
                # Validator nodes start with completely empty database - no genesis creation
                # They will sync everything (including genesis) from the network
                logger.info("Validator node starting with completely empty database - will sync genesis and all blocks from network")
        else:
            # Database has some blocks already
            logger.info(f"Node starting with existing blockchain - best block: {best_hash[:16]}... at height {best_height}")
        
        logger.info("Blockchain components ready")
        
        # Load wallet if specified
        wallet_file = os.environ.get('WALLET_FILE', 'wallet.json')
        wallet_password = os.environ.get('WALLET_PASSWORD')
        if not wallet_password:
            logger.error("WALLET_PASSWORD environment variable is not set. Cannot start node without wallet password.")
            raise RuntimeError("WALLET_PASSWORD environment variable must be set")
        
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
                from web.web import set_gossip_node, set_broadcast_transactions
                set_gossip_node(gossip_node)
                logger.info("Gossip node reference set in web module")
                
                # Configure websocket transaction broadcasting
                set_broadcast_transactions(args.broadcast_transactions)
                logger.info(f"Websocket transaction broadcasting configured: {args.broadcast_transactions}")
            except Exception as e:
                logger.warning(f"Could not set gossip node in web module: {e}")
            
            logger.info("Gossip server started")

            # Reduced wait time - networking can initialize in parallel
            await asyncio.sleep(0.5)
            logger.info("Networking components initialized")

            # Initialize explorer index on startup (incremental only, no rebuild)
            logger.info("Initializing explorer transaction index...")
            from blockchain.explorer_index import get_explorer_index
            explorer_index = get_explorer_index(rebuild=False)  # Don't rebuild on startup!
            logger.info("Explorer index ready")
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
        
        # Shutdown thread pool executors
        from utils.executors import shutdown_executors
        shutdown_executors()

        # Close database connections
        # Database cleanup happens automatically

        logger.info("Node shutdown completed")
        
    except Exception as e:
        logger.error(f"Error during shutdown: {str(e)}")
        # Don't raise during shutdown to allow graceful exit
