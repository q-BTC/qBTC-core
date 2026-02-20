from database.database import get_db, get_current_height, invalidate_height_cache
from rocksdict import WriteBatch
from blockchain.blockchain import Block, calculate_merkle_root, validate_pow, serialize_transaction, sha256d
from blockchain.chain_singleton import get_chain_manager
from blockchain.block_factory import normalize_block
from config.config import ADMIN_ADDRESS, GENESIS_ADDRESS, CHAIN_ID, TX_EXPIRATION_TIME
from blockchain.transaction_validator import TransactionValidator
from blockchain.event_integration import emit_database_event
from state.state import mempool_manager
from events.event_bus import event_bus, EventTypes
from sync.sync_state_manager import get_sync_state_manager
from blockchain.block_height_index import get_height_index
import asyncio
import json
import logging
import time
import traceback
from decimal import Decimal, ROUND_DOWN
from typing import List, Dict, Tuple, Optional, Set

def _cleanup_mempool_after_sync(blocks: list[dict]):
    """Clean up mempool after syncing blocks"""
    try:
        all_tx_ids = set()
        for block in blocks:
            tx_ids = block.get("tx_ids", [])
            if len(tx_ids) > 1:  # Skip coinbase
                all_tx_ids.update(tx_ids[1:])
        
        if not all_tx_ids:
            return
            
        removed_count = 0
        for txid in all_tx_ids:
            if mempool_manager.get_transaction(txid) is not None:
                mempool_manager.remove_transaction(txid)
                removed_count += 1
                logging.debug(f"[SYNC] Removed synced transaction {txid} from mempool")
        
        if removed_count > 0:
            logging.info(f"[SYNC] Post-sync cleanup: removed {removed_count} mined transactions from mempool")
            
    except Exception as e:
        logging.error(f"Error during mempool cleanup: {e}", exc_info=True)

# Track concurrent calls
_processing_lock = None  # Will be created lazily
_processing_count = 0

def _get_processing_lock():
    """Get or create the processing lock in the current event loop"""
    global _processing_lock
    if _processing_lock is None:
        _processing_lock = asyncio.Lock()
    return _processing_lock

async def process_blocks_from_peer(blocks: list[dict]):
    global _processing_count
    _processing_count += 1
    call_id = _processing_count
    
    logging.info(f"***** IN GOSSIP MSG RECEIVE BLOCKS RESPONSE (call #{call_id})")
    logging.info(f"Call #{call_id}: Processing {len(blocks)} blocks")
    
    # Wrap entire function to catch any error
    try:
        result = await _process_blocks_from_peer_impl(blocks)
        logging.info(f"Call #{call_id}: Completed successfully")
        return result
    except Exception as e:
        logging.error(f"CRITICAL ERROR in process_blocks_from_peer call #{call_id}: {e}", exc_info=True)
        # Re-raise to maintain original behavior
        raise

async def _process_blocks_from_peer_impl(blocks: list[dict]):
    """Actual implementation of process_blocks_from_peer"""
    
    # Debug logging to understand block structure
    logging.info(f"_process_blocks_from_peer_impl called with {len(blocks) if isinstance(blocks, list) else 'non-list'} blocks")
    if blocks and isinstance(blocks, list):
        for i, block in enumerate(blocks[:3]):  # Log first 3 blocks
            if isinstance(block, dict):
                height = block.get("height")
                block_hash = block.get("block_hash")
                logging.info(f"Block {i}: height={height} (type: {type(height)}), hash={block_hash}")
                if isinstance(height, str) and len(height) == 64:
                    logging.error(f"Block {i} has hash in height field!")
                    # Log all block fields to understand the corruption
                    for k, v in block.items():
                        if isinstance(v, str) and len(v) < 100:
                            logging.error(f"  {k}: {v}")
                        else:
                            logging.error(f"  {k}: {type(v)}")
    
    try:
        db = get_db()
        cm = await get_chain_manager()
        raw_blocks = blocks

        logging.debug(f"[SYNC] Processing {len(raw_blocks)} blocks from peer")
        
        # Enable sync mode for bulk block processing
        cm.set_sync_mode(True)
        
        # Log the type and structure for debugging
        logging.info(f"Received blocks type: {type(blocks)}")
        if blocks and len(blocks) > 0:
            logging.info(f"First block type: {type(blocks[0])}")
            logging.info(f"First block keys: {list(blocks[0].keys()) if isinstance(blocks[0], dict) else 'Not a dict'}")
            
        if isinstance(raw_blocks, dict):
            raw_blocks = [raw_blocks]

        # Sort blocks by height, handling missing or invalid height values
        def get_height(block):
            height = block.get("height", 0)
            # Ensure height is an integer
            if isinstance(height, int):
                return height
            elif isinstance(height, str):
                try:
                    return int(height)
                except ValueError:
                    logging.error(f"Invalid height value '{height}' in block {block.get('block_hash', 'unknown')}")
                    return -1  # Use -1 to sort invalid blocks first
            else:
                return 0
        
        # Filter out malformed blocks before processing
        valid_blocks = []
        for block in raw_blocks:
            if not isinstance(block, dict):
                logging.error(f"Skipping non-dict block: {type(block)}")
                continue
            
            height = block.get("height")
            block_hash = block.get("block_hash")
            
            # Check if height looks like a block hash
            if isinstance(height, str) and len(height) == 64 and all(c in '0123456789abcdefABCDEF' for c in height):
                logging.error(f"Skipping malformed block with hash in height field: height={height}, hash={block_hash}")
                continue
                
            # Check if block_hash looks valid
            if not isinstance(block_hash, str) or len(block_hash) != 64:
                logging.error(f"Skipping block with invalid hash: {block_hash}")
                continue
                
            valid_blocks.append(block)
        
        blocks = sorted(valid_blocks, key=get_height)
        logging.info("Received %d blocks, %d valid after filtering", len(raw_blocks), len(blocks))
        
        # Check for duplicate blocks
        seen_hashes = set()
        seen_heights = set()
        for i, block in enumerate(blocks):
            block_hash = block.get("block_hash")
            height = block.get("height")
            if block_hash in seen_hashes:
                logging.error(f"Duplicate block hash found at index {i}: {block_hash}")
                logging.error(f"Block {i} data: height={height} (type: {type(height)})")
            if height in seen_heights and height is not None:
                logging.error(f"Duplicate height found at index {i}: {height}")
                logging.error(f"Block {i} hash: {block_hash}")
            seen_hashes.add(block_hash)
            if height is not None:
                seen_heights.add(height)
    except Exception as e:
        logging.error(f"Error in process_blocks_from_peer setup: {e}", exc_info=True)
        raise

    accepted_count = 0
    rejected_count = 0
    
    # Get sync state manager for atomic operations
    sync_manager = get_sync_state_manager()
    
    # Start atomic sync transaction
    block_hashes = [b.get("block_hash") for b in blocks if b.get("block_hash")]
    sync_id = sync_manager.begin_sync_transaction(block_hashes)
    
    try:
        for block in blocks:
            try:
                height = block.get("height")
                block_hash = block.get("block_hash")
                prev_hash = block.get("previous_hash")
                
                # Validate height before processing
                if isinstance(height, str) and len(height) == 64 and all(c in '0123456789abcdefABCDEF' for c in height):
                    logging.error(f"Skipping block with hash in height field: height={height}, block_hash={block_hash}")
                    logging.error(f"Block keys: {list(block.keys())}")
                    rejected_count += 1
                    continue
                
                # Log block structure for debugging
                logging.info(f"Processing block at height {height} (type: {type(height)}) with hash {block_hash}")
                logging.info(f"Block has bits field: {'bits' in block}")
                logging.debug(f"Full block structure: {json.dumps(block, indent=2)}")
                
                # Add full_transactions to block if not present
                if "full_transactions" not in block:
                    block["full_transactions"] = block.get("full_transactions", [])
                
                # Normalize block for consistency before processing
                try:
                    block = normalize_block(block, add_defaults=False)
                except ValueError as e:
                    logging.warning(f"Block normalization failed: {e}")
                    rejected_count += 1
                    continue
                
                # Let ChainManager handle consensus
                logging.debug(f"Calling add_block with height={block.get('height')} (type: {type(block.get('height'))})")
                success, error = await cm.add_block(block)
                
                if success:
                    accepted_count += 1
                    # Record successful block processing in sync transaction
                    # Track what was actually added to the database
                    state_changes = {
                        "block_added": True,  # Block was added to database
                        "block_hash": block_hash,
                        "height": block.get("height"),
                        "transactions": block.get("tx_ids", []),
                        "created_utxos": [],  # Would need to track from add_block
                        "spent_utxos": {}     # Would need to track from add_block
                    }
                    sync_manager.record_block_processed(sync_id, block_hash, state_changes)
                    
                    # ChainManager.add_block() already handles everything:
                    # - Validates all transactions
                    # - Marks UTXOs as spent
                    # - Creates new UTXOs
                    # - Updates wallet indexes
                    # - Stores block and transactions
                    # - Updates height index
                    # No need for additional processing!
                    if await cm.is_block_in_main_chain(block_hash):
                        logging.info("Block %s accepted and confirmed in main chain", block_hash)
                    else:
                        logging.info("Block %s accepted but not in main chain yet (fork/orphan)", block_hash)
                    continue
                else:
                    rejected_count += 1
                    # Record failed block in sync transaction
                    sync_manager.record_block_failed(sync_id, block_hash, error)
                    logging.warning("Block %s rejected: %s", block_hash, error)

                    # Check if this is a missing UTXO error - indicates we need earlier blocks
                    if "references non-existent UTXO" in str(error):
                        logging.warning(f"[SYNC] Block {block_hash} failed due to missing UTXO - need earlier blocks")
                        # Extract the missing UTXO from error message if possible
                        import re
                        utxo_match = re.search(r'UTXO ([a-f0-9]+:[0-9]+)', str(error))
                        if utxo_match:
                            missing_utxo = utxo_match.group(1)
                            logging.info(f"[SYNC] Missing UTXO identified: {missing_utxo}")
                            # Store this as a block that needs earlier dependencies
                            await _handle_missing_utxo(block, missing_utxo)

                    # Even if block is rejected, we should still remove any transactions
                    # from mempool that are in this block (they might be invalid)
                    if "tx_ids" in block and len(block.get("tx_ids", [])) > 1:
                        # Skip coinbase (first transaction)
                        tx_ids_to_check = block["tx_ids"][1:]
                        removed_txids = []
                        for txid in tx_ids_to_check:
                            if mempool_manager.get_transaction(txid) is not None:
                                mempool_manager.remove_transaction(txid)
                                removed_txids.append(txid)
                                # Record mempool removal in sync transaction
                                sync_manager.record_mempool_removal(sync_id, txid)
                                logging.info(f"[SYNC] Removed transaction {txid} from mempool (block rejected)")
                        
                        if removed_txids:
                            logging.info(f"[SYNC] Removed {len(removed_txids)} transactions from mempool after rejected block {block_hash}")
                    
                    continue
                    
            except Exception as e:
                block_hash = block.get("block_hash", "unknown")
                logging.error("Error processing block %s: %s", block_hash, e)
                logging.error("Block data at error: height=%s (type: %s), hash=%s", 
                            block.get("height"), type(block.get("height")), block.get("block_hash"))
                logging.error("Exception type: %s", type(e).__name__)
                logging.error("Full traceback:", exc_info=True)
                rejected_count += 1
                # Record the error in sync transaction
                if block_hash != "unknown":
                    sync_manager.record_block_failed(sync_id, block_hash, str(e))

        logging.info("Block processing complete: %d accepted, %d rejected", accepted_count, rejected_count)
        
        # Commit or rollback the sync transaction based on results
        if rejected_count > 0 and accepted_count == 0:
            # All blocks failed - rollback the transaction
            logging.warning(f"[SYNC] All blocks failed, rolling back sync transaction {sync_id}")
            sync_manager.rollback_sync_transaction(sync_id)
        else:
            # At least some blocks succeeded - commit the transaction
            logging.info(f"[SYNC] Committing sync transaction {sync_id}")
            sync_manager.commit_sync_transaction(sync_id)
        
        # After initial sync, do a final mempool cleanup
        # This ensures any transactions that were mined in blocks we just synced are removed
        if accepted_count > 0:
            _cleanup_mempool_after_sync(blocks)
        
        # Try to connect orphan blocks that may now be connectable
        if rejected_count > 0:
            logging.info("[SYNC] Attempting to connect orphan blocks...")
            orphans_connected = await cm.try_connect_orphan_chain()
            if orphans_connected:
                logging.info("[SYNC] Successfully connected some orphan blocks")
                accepted_count += 1  # Mark that we made progress
        
        # Check if we need to request more blocks
        best_tip, best_height = await cm.get_best_chain_tip()
        logging.info("Current best chain height: %d", best_height)
        
        # Check for orphan chains that need ancestor blocks
        # We need to run this in an async context
        try:
            # Get the gossip node reference
            from web.web import get_gossip_node
            gossip_client = get_gossip_node()
            
            if gossip_client:
                # Check if we're already in an async context
                try:
                    loop = asyncio.get_running_loop()
                    # We're in an async context, create a task
                    task = asyncio.create_task(_check_orphan_chains_for_missing_blocks(gossip_client))
                    # Don't wait for it to complete - let it run in background
                    logging.info("[SYNC] Scheduled orphan chain check in background")
                except RuntimeError:
                    # No running loop, create a new one
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                    try:
                        loop.run_until_complete(_check_orphan_chains_for_missing_blocks(gossip_client))
                    finally:
                        loop.close()
            else:
                logging.warning("[SYNC] No gossip client available, skipping orphan chain check")
        except Exception as e:
            logging.error(f"[SYNC] Error checking orphan chains: {e}")
        
        return accepted_count > 0
    
    except Exception as e:
        # If there's an unexpected error, rollback the sync transaction
        logging.error(f"[SYNC] Unexpected error during sync, rolling back transaction {sync_id}: {e}")
        sync_manager.rollback_sync_transaction(sync_id)
        raise
    
    finally:
        # Always disable sync mode after processing
        cm.set_sync_mode(False)

async def get_blockchain_info() -> Dict:
    """Get current blockchain information"""
    cm = await get_chain_manager()
    best_hash, best_height = await cm.get_best_chain_tip()
    
    return {
        "best_block_hash": best_hash,
        "height": best_height,
        "chain_tips": list(cm.chain_tips),
        "orphan_count": len(cm.orphan_blocks),
        "index_size": len(cm.block_index)
    }

async def _check_orphan_chains_for_missing_blocks(gossip_client=None):
    """Check if any orphan chains need ancestor blocks to be requested"""
    cm = await get_chain_manager()
    
    # Track blocks we need to request
    blocks_to_request: Set[Tuple[str, int]] = set()
    
    # Check each orphan for missing ancestors
    for orphan_hash in cm.orphan_blocks:
        ancestor_info = await cm.request_missing_ancestor(orphan_hash)
        if ancestor_info:
            blocks_to_request.add(ancestor_info)
    
    if blocks_to_request:
        logging.warning(f"[SYNC] Found {len(blocks_to_request)} missing ancestor blocks needed for orphan chains")
        
        # Extract just the block hashes for the request
        block_hashes_to_request = []
        for block_hash, height in blocks_to_request:
            logging.warning(f"[SYNC] Need to request block at height {height} with hash {block_hash}")
            block_hashes_to_request.append(block_hash)
        
        # Actually request the missing blocks
        if gossip_client:
            received_blocks = await request_specific_blocks(block_hashes_to_request, gossip_client)
            if received_blocks:
                logging.info(f"[SYNC] Successfully requested {len(received_blocks)} missing blocks")
                # Process the received blocks
                await process_blocks_from_peer(received_blocks)
            else:
                logging.error(f"[SYNC] Failed to request missing blocks")
        else:
            logging.warning("[SYNC] No gossip client available to request missing blocks")

# Track blocks with missing UTXOs to avoid infinite retry loops
_missing_utxo_blocks = {}  # block_hash -> {"height": int, "attempts": int, "missing_utxos": set()}
_max_backtrack_attempts = 3  # Maximum times to try requesting earlier blocks
_max_recursion_depth = 3  # Maximum recursive calls to process_blocks_from_peer
_current_recursion_depth = 0  # Track current recursion depth

async def _handle_missing_utxo(block: dict, missing_utxo: str):
    """
    Handle a block that failed due to missing UTXO by requesting earlier blocks.
    """
    block_hash = block.get("block_hash")
    block_height = block.get("height", 0)

    # Track this failed block
    if block_hash not in _missing_utxo_blocks:
        _missing_utxo_blocks[block_hash] = {
            "height": block_height,
            "attempts": 0,
            "missing_utxos": set()
        }

    _missing_utxo_blocks[block_hash]["missing_utxos"].add(missing_utxo)
    _missing_utxo_blocks[block_hash]["attempts"] += 1

    # SECURITY: Never fabricate UTXOs from individual transaction requests.
    # Individual transactions have no PoW proof — a malicious peer could send
    # fabricated transactions to create fake UTXOs. Instead, always request
    # the ancestor BLOCKS and process them through the validated add_block
    # pipeline (PoW check, signature verification, amount validation, etc.).

    # Check if we've tried too many times
    if _missing_utxo_blocks[block_hash]["attempts"] >= _max_backtrack_attempts:
        logging.error(f"[SYNC] Block {block_hash} at height {block_height} has failed {_max_backtrack_attempts} times due to missing UTXOs")
        logging.error(f"[SYNC] Missing UTXOs: {_missing_utxo_blocks[block_hash]['missing_utxos']}")
        logging.error(f"[SYNC] This node needs a full resync from an earlier point or from genesis")
        return

    # Calculate how far back to request
    # Start by requesting 10 blocks before the failed block
    backtrack_depth = 10 * _missing_utxo_blocks[block_hash]["attempts"]  # Increase depth with each attempt
    start_height = max(0, block_height - backtrack_depth)

    logging.info(f"[SYNC] Attempting to get earlier blocks from height {start_height} to {block_height - 1} (attempt {_missing_utxo_blocks[block_hash]['attempts']})")

    # Request earlier blocks
    from gossip.gossip import get_gossip_node
    gossip_client = get_gossip_node()

    if gossip_client:
        try:
            # Request blocks from earlier height
            blocks_request = {
                "type": "get_blocks",
                "start_height": start_height,
                "end_height": block_height - 1
            }

            # Find a peer to request from
            all_peers = list(gossip_client.dht_peers.union(gossip_client.client_peers))
            if all_peers:
                import random
                peer = random.choice(all_peers)

                logging.info(f"[SYNC] Requesting blocks {start_height} to {block_height - 1} from peer {peer}")

                reader, writer = await asyncio.open_connection(peer[0], peer[1])
                writer.write((json.dumps(blocks_request) + "\n").encode('utf-8'))
                await writer.drain()

                # Read response
                response = await reader.readline()
                if response:
                    msg = json.loads(response.decode('utf-8'))
                    if msg.get("type") == "blocks_response":
                        blocks = msg.get("blocks", [])
                        if blocks:
                            logging.info(f"[SYNC] Received {len(blocks)} earlier blocks, processing them first")
                            # Process these earlier blocks first with recursion depth guard
                            if _current_recursion_depth >= _max_recursion_depth:
                                logging.error(f"[SYNC] Max recursion depth ({_max_recursion_depth}) reached, aborting backtrack")
                            else:
                                _current_recursion_depth += 1
                                try:
                                    await process_blocks_from_peer(blocks)
                                finally:
                                    _current_recursion_depth -= 1
                        else:
                            logging.warning(f"[SYNC] No blocks received for range {start_height} to {block_height - 1}")

                writer.close()
                await writer.wait_closed()

        except Exception as e:
            logging.error(f"[SYNC] Failed to request earlier blocks: {e}")

async def request_specific_blocks(block_hashes: List[str], gossip_client=None) -> Optional[List[dict]]:
    """
    Request specific blocks by hash from peers.
    Returns the blocks if successful, None otherwise.
    """
    if not block_hashes:
        return None

    if not gossip_client:
        logging.error("[SYNC] No gossip client provided for block requests")
        return None
    
    logging.info(f"[SYNC] Requesting {len(block_hashes)} specific blocks from peers")
    
    # Create request message
    request_msg = {
        "type": "get_blocks_by_hash",
        "block_hashes": block_hashes,
        "timestamp": int(time.time() * 1000)
    }
    
    # Try to request from multiple peers
    peers = list(gossip_client.dht_peers | gossip_client.client_peers)
    if not peers:
        logging.warning("[SYNC] No peers available to request blocks from")
        return None
    
    received_blocks = []
    
    # Try up to 3 different peers
    for i, peer in enumerate(peers[:3]):
        try:
            logging.info(f"[SYNC] Requesting blocks from peer {peer}")
            
            # Send request to peer
            response = await gossip_client.send_message(peer, request_msg)
            
            if response and response.get("type") == "blocks_by_hash_response":
                blocks = response.get("blocks", [])
                if blocks:
                    logging.info(f"[SYNC] Received {len(blocks)} blocks from peer {peer}")
                    received_blocks.extend(blocks)
                    
                    # If we got all requested blocks, return
                    if len(received_blocks) >= len(block_hashes):
                        return received_blocks[:len(block_hashes)]
            
        except Exception as e:
            logging.warning(f"[SYNC] Failed to request blocks from peer {peer}: {e}")
            continue
    
    if received_blocks:
        logging.info(f"[SYNC] Received {len(received_blocks)} out of {len(block_hashes)} requested blocks")
        return received_blocks
    else:
        logging.warning(f"[SYNC] Failed to receive any of the {len(block_hashes)} requested blocks")
        return None

async def request_specific_transactions(tx_ids: List[str], gossip_client=None) -> Optional[List[dict]]:
    """
    Request specific transactions by ID from peers.
    Returns the transactions if successful, None otherwise.
    """
    if not tx_ids:
        return None

    if not gossip_client:
        logging.error("[SYNC] No gossip client provided for transaction requests")
        return None

    logging.info(f"[SYNC] Requesting {len(tx_ids)} specific transactions from peers")

    # Try to request from multiple peers
    peers = list(gossip_client.dht_peers | gossip_client.client_peers)
    if not peers:
        logging.warning("[SYNC] No peers available to request transactions from")
        return None

    received_txs = []

    # Try up to 3 different peers
    for peer in peers[:3]:
        try:
            logging.info(f"[SYNC] Requesting transactions from peer {peer}")

            # Create connection
            reader, writer = await asyncio.open_connection(peer[0], peer[1])

            # Send request
            request_msg = {
                "type": "get_transactions",
                "tx_ids": tx_ids,
                "timestamp": int(time.time() * 1000)
            }
            writer.write((json.dumps(request_msg) + "\n").encode('utf-8'))
            await writer.drain()

            # Read response
            response_data = await reader.readline()
            if response_data:
                response = json.loads(response_data.decode('utf-8'))
                if response.get("type") == "transactions_response":
                    txs = response.get("transactions", [])
                    if txs:
                        logging.info(f"[SYNC] Received {len(txs)} transactions from peer {peer}")
                        received_txs.extend(txs)

                        # If we got all requested transactions, return
                        if len(received_txs) >= len(tx_ids):
                            writer.close()
                            await writer.wait_closed()
                            return received_txs[:len(tx_ids)]

            writer.close()
            await writer.wait_closed()

        except Exception as e:
            logging.warning(f"[SYNC] Failed to request transactions from peer {peer}: {e}")
            continue

    if received_txs:
        logging.info(f"[SYNC] Received {len(received_txs)} out of {len(tx_ids)} requested transactions")
        return received_txs
    else:
        logging.warning(f"[SYNC] Failed to receive any of the {len(tx_ids)} requested transactions")
        return None