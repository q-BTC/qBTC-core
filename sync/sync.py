from database.database import get_db, get_current_height
from rocksdict import WriteBatch
from blockchain.blockchain import Block, calculate_merkle_root, validate_pow, serialize_transaction, sha256d
from blockchain.chain_singleton import get_chain_manager
from config.config import ADMIN_ADDRESS, GENESIS_ADDRESS, CHAIN_ID, TX_EXPIRATION_TIME
from wallet.wallet import verify_transaction
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

        print("**** RAW BLOCkS ****")
        print(raw_blocks)
        
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
    
    # Track total fees collected in this block
    total_fees = Decimal("0")
    # Track spent UTXOs within this block to prevent double-spending
    spent_in_block = set()
    # Store coinbase data for validation after fee calculation
    coinbase_data = None

    for raw in full_transactions:
        if raw is None:
            continue
        tx = raw
        if "transaction" in tx:
            tx = tx["transaction"]
        if tx.get("txid") == "genesis_tx":
            logging.debug("[SYNC] Genesis transaction detected")
            continue

        is_probable_coinbase = all(k in tx for k in ("version", "inputs", "outputs")) and not tx.get("txid")
        if is_probable_coinbase:
            logging.debug("[SYNC] Coinbase transaction detected")
            coinbase_tx_id = f"coinbase_{height}"
            
            # Store coinbase data for validation after processing all transactions
            coinbase_total = Decimal("0")
            coinbase_outputs = []
            
            for idx, output in enumerate(tx.get("outputs", [])):
                output_amount = Decimal(str(output.get("value", "0")))
                coinbase_total += output_amount
                
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
            
            # Store coinbase data for validation after fee calculation
            coinbase_data = {
                "tx": tx,
                "tx_id": coinbase_tx_id,
                "total": coinbase_total,
                "outputs": coinbase_outputs
            }
            continue

        if "txid" in tx:
            txid = tx["txid"]
            inputs = tx.get("inputs", [])
            outputs = tx.get("outputs", [])
            body = tx.get("body", {})

            pubkey = body.get("pubkey", "unknown")
            signature = body.get("signature", "unknown")

            from_ = to_ = total_authorized = time_ = None
            if body.get("transaction_data") == "initial_distribution" and height == 1:
                total_authorized = "21000000"  
                to_ = ADMIN_ADDRESS
                from_ = GENESIS_ADDRESS
            else:
                msg_str = body.get("msg_str", "")
                parts = msg_str.split(":")
                # MANDATORY format: sender:receiver:amount:timestamp:chain_id
                if len(parts) != 5:
                    raise ValueError(f"Transaction {txid} invalid format - must have sender:receiver:amount:timestamp:chain_id")
                from_, to_, total_authorized, time_, tx_chain_id = parts
                
                # Validate chain ID
                if int(tx_chain_id) != CHAIN_ID:
                    raise ValueError(f"Invalid chain ID in tx {txid}: expected {CHAIN_ID}, got {tx_chain_id}")
                
                # Validate timestamp for expiration
                try:
                    tx_timestamp = int(time_)
                    current_time = int(time.time() * 1000)  # Convert to milliseconds
                    tx_age = (current_time - tx_timestamp) / 1000  # Age in seconds
                    
                    if tx_age > TX_EXPIRATION_TIME:
                        raise ValueError(f"Transaction {txid} expired: age {tx_age}s > max {TX_EXPIRATION_TIME}s")
                    
                    # Reject transactions with future timestamps (more than 5 minutes in the future)
                    if tx_age < -300:  # -300 seconds = 5 minutes in the future
                        raise ValueError(f"Transaction {txid} has future timestamp: {-tx_age}s in the future")
                        
                except (ValueError, TypeError) as e:
                    raise ValueError(f"Invalid timestamp in tx {txid}: {time_}")

            total_available = Decimal("0")
            total_required = Decimal("0")

            for inp in inputs:
                if "txid" not in inp:
                    continue
                spent_key = f"utxo:{inp['txid']}:{inp.get('utxo_index', 0)}".encode()
                spent_key_str = spent_key.decode()
                
                # Check if this UTXO was already spent in this block
                if spent_key_str in spent_in_block:
                    raise ValueError(f"Double spend detected: UTXO {spent_key_str} already spent in this block")
                
                utxo_raw = db.get(spent_key)
                if not utxo_raw:
                    raise ValueError(f"Missing UTXO for input: {spent_key}")
                utxo = json.loads(utxo_raw.decode())

                if utxo["spent"]:
                    raise ValueError(f"UTXO {spent_key} already spent")
                if utxo["receiver"] != from_:
                    raise ValueError(f"UTXO {spent_key} not owned by sender {from_}")

                total_available += Decimal(utxo["amount"])
                # Mark as spent in this block
                spent_in_block.add(spent_key_str)

            total_to_recipient = Decimal("0")
            total_change = Decimal("0")
            
            for out in outputs:
                recv = out.get("receiver")
                amt = Decimal(out.get("amount", "0"))
                print(out)
                print("receiver:")
                print(recv)
                print("to:")
                print(to_)
                print(ADMIN_ADDRESS)
                if recv == to_:
                    total_to_recipient += amt
                elif recv == from_:
                    total_change += amt
                elif recv == ADMIN_ADDRESS:
                    # This is fee to admin
                    total_required += amt
                else:
                    raise ValueError(
                        f"Hack detected: unauthorized output to {recv} in tx {txid}")
                total_required += amt

            miner_fee = (Decimal(total_authorized) * Decimal("0.001")).quantize(
                Decimal("0.00000001"), rounding=ROUND_DOWN)
            grand_total_required = Decimal(total_authorized) + miner_fee

            if height > 1 and grand_total_required > total_available:
                raise ValueError(
                    f"Invalid tx {txid}: balance {total_available} < required {grand_total_required}")
            
            # Fix 3: Enforce exact payment amount to recipient
            if height > 1 and total_to_recipient != Decimal(total_authorized):
                raise ValueError(
                    f"Invalid tx {txid}: authorized amount {total_authorized} != amount sent to recipient {total_to_recipient}")

            if height != 1 and not verify_transaction(msg_str, signature, pubkey):
                raise ValueError(f"Signature check failed for tx {txid}")
            
            # Calculate the actual transaction fee for this transaction
            if height > 1:
                tx_fee = total_available - (total_to_recipient + total_change)
                total_fees += tx_fee

            batch.put(f"tx:{txid}".encode(), json.dumps(tx).encode())

            for inp in inputs:
                if "txid" not in inp:
                    continue
                spent_key = f"utxo:{inp['txid']}:{inp.get('utxo_index', 0)}".encode()
                if spent_key in db:
                    utxo_rec = json.loads(db.get(spent_key).decode())
                    utxo_rec["spent"] = True
                    batch.put(spent_key, json.dumps(utxo_rec).encode())

  
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

    # Fix 1: Validate coinbase amount after all fees are calculated
    if coinbase_data is not None:
        # Define block reward schedule
        # Bitcoin-like halving schedule: 50 BTC initially, halving every 210,000 blocks
        halvings = height // 210000
        if halvings >= 64:
            block_subsidy = Decimal("0")
        else:
            block_subsidy = Decimal("50") / (2 ** halvings) * Decimal("100000000")  # In satoshis
        
        # Maximum allowed coinbase output
        max_coinbase_amount = block_subsidy + total_fees
        
        logging.info(f"[SYNC] Validating coinbase: total={coinbase_data['total']}, subsidy={block_subsidy}, fees={total_fees}, max_allowed={max_coinbase_amount}")
        
        if coinbase_data['total'] > max_coinbase_amount:
            raise ValueError(
                f"Invalid coinbase amount at height {height}: {coinbase_data['total']} > allowed {max_coinbase_amount} (subsidy={block_subsidy} + fees={total_fees})")
        
        # Now that coinbase is validated, store it
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