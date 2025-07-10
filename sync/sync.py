from database.database import get_db, get_current_height
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
from typing import List, Dict, Tuple, Optional

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

def process_blocks_from_peer(blocks: list[dict]):
    global _processing_count
    _processing_count += 1
    call_id = _processing_count
    
    logging.info(f"***** IN GOSSIP MSG RECEIVE BLOCKS RESPONSE (call #{call_id})")
    logging.info(f"Call #{call_id}: Processing {len(blocks)} blocks")
    
    # Wrap entire function to catch any error
    try:
        result = _process_blocks_from_peer_impl(blocks)
        logging.info(f"Call #{call_id}: Completed successfully")
        return result
    except Exception as e:
        logging.error(f"CRITICAL ERROR in process_blocks_from_peer call #{call_id}: {e}", exc_info=True)
        # Re-raise to maintain original behavior
        raise

def _process_blocks_from_peer_impl(blocks: list[dict]):
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
        cm = get_chain_manager()
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
                success, error = cm.add_block(block)
                
                if success:
                    accepted_count += 1
                    # Only process if block is in main chain
                    if cm.is_block_in_main_chain(block_hash):
                        # Process the block transactions
                        _process_block_in_chain(block)
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
        
        # Check if we need to request more blocks
        best_tip, best_height = cm.get_best_chain_tip()
        logging.info("Current best chain height: %d", best_height)
        
        return accepted_count > 0
    
    finally:
        # Always disable sync mode after processing
        cm.set_sync_mode(False)

def _process_block_in_chain(block: dict):
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
            coinbase_tx_id = f"coinbase_{height}"
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
                # Generate coinbase txid
                tx_ids.append(f"coinbase_{height}")
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

def get_blockchain_info() -> Dict:
    """Get current blockchain information"""
    cm = get_chain_manager()
    best_hash, best_height = cm.get_best_chain_tip()
    
    return {
        "best_block_hash": best_hash,
        "height": best_height,
        "chain_tips": list(cm.chain_tips),
        "orphan_count": len(cm.orphan_blocks),
        "index_size": len(cm.block_index)
    }