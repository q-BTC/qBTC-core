from database.database import get_db, get_current_height
from rocksdict import WriteBatch
from blockchain.blockchain import Block, calculate_merkle_root, validate_pow, serialize_transaction, sha256d
from blockchain.chain_singleton import get_chain_manager
from config.config import ADMIN_ADDRESS, GENESIS_ADDRESS, CHAIN_ID, TX_EXPIRATION_TIME
from blockchain.transaction_validator import TransactionValidator
from blockchain.event_integration import emit_database_event
from state.state import mempool_manager
from events.event_bus import event_bus, EventTypes
import asyncio
import json
import logging
import time
from decimal import Decimal, ROUND_DOWN
from typing import List, Dict, Tuple, Optional

def process_blocks_from_peer(blocks: list[dict]):
    logging.info("***** IN GOSSIP MSG RECEIVE BLOCKS RESPONSE")
    
    # Wrap entire function to catch any error
    try:
        return _process_blocks_from_peer_impl(blocks)
    except Exception as e:
        logging.error(f"CRITICAL ERROR in process_blocks_from_peer: {e}", exc_info=True)
        # Re-raise to maintain original behavior
        raise

def _process_blocks_from_peer_impl(blocks: list[dict]):
    """Actual implementation of process_blocks_from_peer"""
    
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
            if isinstance(height, str):
                # Check if this looks like a block hash (64 hex chars)
                if len(height) == 64 and all(c in '0123456789abcdefABCDEF' for c in height):
                    logging.error(f"Block hash '{height}' found in height field for block {block.get('block_hash', 'unknown')}")
                    logging.error(f"Full block data: {block}")
                    return 0
                try:
                    return int(height)
                except ValueError:
                    logging.warning(f"Invalid height value '{height}' in block {block.get('block_hash', 'unknown')}")
                    return 0
            return int(height) if height is not None else 0
        
        blocks = sorted(raw_blocks, key=get_height)
        logging.info("Received %d blocks", len(blocks))
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
                
                # Log block structure for debugging
                logging.info(f"Processing block at height {height} with hash {block_hash}")
                logging.info(f"Block has bits field: {'bits' in block}")
                
                # Add full_transactions to block if not present
                if "full_transactions" not in block:
                    block["full_transactions"] = block.get("full_transactions", [])
                
                # Let ChainManager handle consensus
                success, error = cm.add_block(block)
                
                if success:
                    accepted_count += 1
                    # Only process if block is in main chain
                    if cm.is_block_in_main_chain(block_hash):
                        # Process the block transactions
                        _process_block_in_chain(block)
                    else:
                        logging.info("Block %s accepted but not in main chain yet", block_hash)
                else:
                    rejected_count += 1
                    logging.warning("Block %s rejected: %s", block_hash, error)
                    continue
                    
            except Exception as e:
                logging.error("Error processing block %s: %s", block.get("block_hash", "unknown"), e)
                rejected_count += 1

        logging.info("Block processing complete: %d accepted, %d rejected", accepted_count, rejected_count)
        
        # Check if we need to request more blocks
        best_tip, best_height = cm.get_best_chain_tip()
        logging.info("Current best chain height: %d", best_height)
        
        return accepted_count > 0
    
    finally:
        # Always disable sync mode after processing
        cm.set_sync_mode(False)

def _process_block_in_chain(block: dict):
    """Process a block that is confirmed to be in the main chain"""
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
    
    # Remove transactions from mempool
    # Skip the first tx_id as it's the coinbase transaction
    
    # Remove confirmed transactions using mempool manager
    confirmed_txids = tx_ids[1:]  # Skip coinbase (first transaction)
    
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