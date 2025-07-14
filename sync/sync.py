from database.database import get_db, get_current_height, invalidate_height_cache
from rocksdict import WriteBatch
from blockchain.blockchain import Block, calculate_merkle_root, validate_pow, serialize_transaction, sha256d
from blockchain.chain_singleton import get_chain_manager
from config.config import ADMIN_ADDRESS, GENESIS_ADDRESS, CHAIN_ID, TX_EXPIRATION_TIME
from blockchain.transaction_validator import TransactionValidator
from blockchain.event_integration import emit_database_event
from state.state import mempool_manager
from events.event_bus import event_bus, EventTypes
from blockchain.block_height_index import get_height_index
import asyncio
import json
import logging
import time
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
_processing_lock = asyncio.Lock()
_processing_count = 0

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
                
                # Let ChainManager handle consensus
                logging.debug(f"Calling add_block with height={block.get('height')} (type: {type(block.get('height'))})")
                success, error = await cm.add_block(block)
                
                if success:
                    accepted_count += 1
                    # Only process if block is in main chain
                    if await cm.is_block_in_main_chain(block_hash):
                        # Process the block transactions
                        await _process_block_in_chain(block)
                    else:
                        logging.info("Block %s accepted but not in main chain yet", block_hash)
                    continue
                else:
                    rejected_count += 1
                    logging.warning("Block %s rejected: %s", block_hash, error)
                    
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
                                logging.info(f"[SYNC] Removed transaction {txid} from mempool (block rejected)")
                        
                        if removed_txids:
                            logging.info(f"[SYNC] Removed {len(removed_txids)} transactions from mempool after rejected block {block_hash}")
                    
                    continue
                    
            except Exception as e:
                logging.error("Error processing block %s: %s", block.get("block_hash", "unknown"), e)
                logging.error("Block data at error: height=%s (type: %s), hash=%s", 
                            block.get("height"), type(block.get("height")), block.get("block_hash"))
                logging.error("Exception type: %s", type(e).__name__)
                logging.error("Full traceback:", exc_info=True)
                rejected_count += 1

        logging.info("Block processing complete: %d accepted, %d rejected", accepted_count, rejected_count)
        
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
    
    finally:
        # Always disable sync mode after processing
        cm.set_sync_mode(False)

async def _process_block_in_chain(block: dict):
    """Process a block that is confirmed to be in the main chain"""
    # Validate input type
    if not isinstance(block, dict):
        raise TypeError(f"Expected dict for block, got {type(block)}: {block}")
    
    db = get_db()
    batch = WriteBatch()
    
    height = block.get("height")
    block_hash = block.get("block_hash")
    prev_hash = block.get("previous_hash")
    tx_ids = block.get("tx_ids", [])
    nonce = block.get("nonce")
    timestamp = block.get("timestamp")
    miner_address = block.get("miner_address")
    full_transactions = block.get("full_transactions", [])
    block_merkle_root = block.get("merkle_root")
    version = block.get("version")
    bits = block.get("bits")
    
    # Validate height is a proper integer
    if isinstance(height, str):
        try:
            height = int(height)
        except ValueError:
            # Check if this looks like a block hash (64 hex chars)
            if len(height) == 64 and all(c in '0123456789abcdefABCDEF' for c in height):
                raise ValueError(f"Block hash '{height}' found in height field for block {block_hash}")
            else:
                raise ValueError(f"Invalid height value '{height}' in block {block_hash}")
    elif height is None:
        raise ValueError(f"Missing height in block {block_hash}")
    elif not isinstance(height, int):
        raise ValueError(f"Height must be an integer, got {type(height)} for block {block_hash}")
    
    logging.info("[SYNC] Processing confirmed block height %s with hash %s", height, block_hash)
    logging.info("[SYNC] Block has %d full transactions", len(full_transactions))
    
    # Create transaction validator with sync mode enabled (skip time validation)
    validator = TransactionValidator(db)
    validator.skip_time_validation = True  # CRITICAL: Set this for historical blocks during sync
    
    # First validate all transactions in the block
    is_valid, error_msg, total_fees = validator.validate_block_transactions(block)
    if not is_valid:
        raise ValueError(f"Block {block_hash} contains invalid transactions: {error_msg}")
    
    # Track spent UTXOs within this block to prevent double-spending
    spent_in_block = set()
    # Store coinbase data for validation after fee calculation
    coinbase_data = None

    # Find and validate coinbase transaction separately
    for tx in full_transactions:
        if tx and validator._is_coinbase_transaction(tx):
            # Use the actual txid from the coinbase transaction
            if "txid" not in tx:
                raise ValueError(f"Coinbase transaction missing txid in block {height}")
            coinbase_tx_id = tx["txid"]
            
            is_valid, error_msg = validator.validate_coinbase_transaction(tx, height, total_fees)
            if not is_valid:
                raise ValueError(f"Invalid coinbase transaction: {error_msg}")
            
            # Store coinbase outputs for later processing
            coinbase_outputs = []
            for idx, output in enumerate(tx.get("outputs", [])):
                output_amount = Decimal(str(output.get("value", "0")))
                output_key = f"utxo:{coinbase_tx_id}:{idx}".encode()
                utxo = {
                    "txid": coinbase_tx_id,
                    "utxo_index": idx,
                    "sender": "coinbase",
                    "receiver": miner_address,
                    "amount": str(output_amount),
                    "spent": False,
                }
                coinbase_outputs.append((output_key, utxo))
            
            coinbase_data = {
                "tx": tx,
                "tx_id": coinbase_tx_id,
                "outputs": coinbase_outputs
            }
            break
    
    # Now process all transactions (they've already been validated)
    for raw in full_transactions:
        if raw is None:
            continue
        tx = raw
        if tx.get("txid") == "genesis_tx":
            logging.debug("[SYNC] Genesis transaction detected")
            continue

        # Skip coinbase (already processed above)
        if validator._is_coinbase_transaction(tx):
            continue

        # All transactions MUST have a txid
        if "txid" not in tx:
            logging.warning(f"[SYNC] Skipping transaction without txid in block {height}")
            continue
        
        txid = tx["txid"]
        
        inputs = tx.get("inputs", [])
        outputs = tx.get("outputs", [])

        batch.put(f"tx:{txid}".encode(), json.dumps(tx).encode())

        # Mark spent UTXOs
        for inp in inputs:
            if "txid" not in inp and "prev_txid" not in inp:
                continue
            # Handle both formats
            inp_txid = inp.get('txid') or inp.get('prev_txid')
            inp_index = inp.get('utxo_index', inp.get('prev_index', 0))
            
            spent_key = f"utxo:{inp_txid}:{inp_index}".encode()
            if spent_key in db:
                utxo_rec = json.loads(db.get(spent_key).decode())
                utxo_rec["spent"] = True
                batch.put(spent_key, json.dumps(utxo_rec).encode())

        # Create new UTXOs
        for out in outputs:
            # Create proper UTXO record with all necessary fields
            utxo_record = {
                "txid": txid,
                "utxo_index": out.get('utxo_index', 0),
                "sender": out.get('sender', ''),
                "receiver": out.get('receiver', ''),
                "amount": str(out.get('amount', '0')),  # Ensure string to avoid scientific notation
                "spent": False  # New UTXOs are always unspent
            }
            out_key = f"utxo:{txid}:{out.get('utxo_index', 0)}".encode()
            batch.put(out_key, json.dumps(utxo_record).encode())

    # Store coinbase transaction and outputs
    if coinbase_data is not None:
        batch.put(f"tx:{coinbase_data['tx_id']}".encode(), json.dumps(coinbase_data['tx']).encode())
        for output_key, utxo in coinbase_data['outputs']:
            batch.put(output_key, json.dumps(utxo).encode())
    
    calculated_root = calculate_merkle_root(tx_ids)
    if calculated_root != block_merkle_root:
        raise ValueError(
            f"Merkle root mismatch at height {height}: {calculated_root} != {block_merkle_root}")


    block_record = {
        "height": height,
        "block_hash": block_hash,
        "previous_hash": prev_hash,
        "tx_ids": tx_ids,
        "nonce": nonce,
        "timestamp": timestamp,
        "miner_address": miner_address,
        "merkle_root": calculated_root,
        "version": version,
        "bits": bits,
    }
    
    # Store the block (ChainManager already validated it)
    block_key = f"block:{block_hash}".encode()
    batch.put(block_key, json.dumps(block_record).encode())
    
    db.write(batch)
    logging.info("[SYNC] Stored block %s (height %s) successfully", block_hash, height)
    
    # Update the height index
    height_index = get_height_index()
    height_index.add_block_to_index(height, block_hash)
    
    # Invalidate height cache since we added a new block
    invalidate_height_cache()
    
    # Remove transactions from mempool
    # Skip the first tx_id as it's the coinbase transaction
    
    # Remove confirmed transactions using mempool manager
    logging.info(f"[SYNC] Block has tx_ids: {tx_ids}")
    
    # If tx_ids is empty but we have full_transactions, extract tx_ids from them
    if not tx_ids and full_transactions:
        tx_ids = []
        for tx in full_transactions:
            if tx and "txid" in tx:
                tx_ids.append(tx["txid"])
            elif tx and validator._is_coinbase_transaction(tx):
                # Coinbase should have txid, if not it's an error
                logging.error(f"[SYNC] Coinbase transaction missing txid in block {height}")
        logging.info(f"[SYNC] Extracted tx_ids from full_transactions: {tx_ids}")
    
    confirmed_txids = tx_ids[1:] if len(tx_ids) > 1 else []  # Skip coinbase (first transaction)
    
    # Track which transactions were actually in our mempool before removal
    confirmed_from_mempool = []
    for txid in confirmed_txids:
        if mempool_manager.get_transaction(txid) is not None:
            confirmed_from_mempool.append(txid)
            logging.debug(f"[SYNC] Transaction {txid} was in our mempool")
        else:
            logging.debug(f"[SYNC] Transaction {txid} not in mempool (might be from another node)")
    
    # Now remove them
    mempool_manager.remove_confirmed_transactions(confirmed_txids)
    
    if confirmed_from_mempool:
        logging.info(f"[SYNC] Removed {len(confirmed_from_mempool)} transactions from mempool after block {block_hash}")
    
    # Emit confirmation events for transactions that were in mempool
    for txid in confirmed_from_mempool:
        # Get transaction data
        tx_key = f"tx:{txid}".encode()
        if tx_key in db:
            tx_data = json.loads(db.get(tx_key).decode())
            # Extract transaction details for the event
            sender = None
            receiver = None
            for output in tx_data.get("outputs", []):
                if output.get("sender"):
                    sender = output["sender"]
                if output.get("receiver"):
                    receiver = output["receiver"]
            
            # Emit transaction confirmed event
            asyncio.create_task(event_bus.emit(EventTypes.TRANSACTION_CONFIRMED, {
                'txid': txid,
                'transaction': {
                    'id': txid,
                    'hash': txid,
                    'sender': sender,
                    'receiver': receiver,
                    'blockHeight': height,
                },
                'blockHeight': height,
                'confirmed_from_mempool': True
            }, source='sync'))
            
            logging.info(f"[SYNC] Emitted TRANSACTION_CONFIRMED event for {txid}")
    
    # Emit events for all database operations
    # Emit transaction events
    for txid in tx_ids:
        tx_key = f"tx:{txid}".encode()
        if tx_key in db:
            emit_database_event(tx_key, db.get(tx_key))
    
    # Emit UTXO events - use full_transactions instead of undefined block_transactions
    for tx in full_transactions:
        if tx and "txid" in tx:
            txid = tx["txid"]
            for out in tx.get("outputs", []):
                out_key = f"utxo:{txid}:{out.get('utxo_index', 0)}".encode()
                if out_key in db:
                    emit_database_event(out_key, db.get(out_key))
    
    # Emit block event
    emit_database_event(block_key, db.get(block_key))

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

async def detect_and_handle_fork(blocks: List[dict]) -> bool:
    """
    Detect if received blocks indicate we're on a different fork.
    Returns True if a fork was detected and handled.
    """
    cm = await get_chain_manager()
    fork_detected = False
    
    for block in blocks:
        height = block.get("height", 0)
        block_hash = block.get("block_hash")
        prev_hash = block.get("previous_hash")
        
        # Check if we have a different block at this height
        our_block_at_height = await cm.detect_fork_at_height(height)
        
        if our_block_at_height and our_block_at_height != block_hash:
            # We have a different block at this height - fork detected!
            logging.warning(f"[SYNC] Fork detected at height {height}!")
            logging.warning(f"[SYNC] Our block: {our_block_at_height}")
            logging.warning(f"[SYNC] Their block: {block_hash}")
            fork_detected = True
            
            # The chain manager will handle reorganization when orphan chains are evaluated
            # We just need to ensure the blocks are added as orphans
    
    return fork_detected